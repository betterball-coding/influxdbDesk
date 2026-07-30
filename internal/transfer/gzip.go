package transfer

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type ArtifactMeta struct {
	PartPath         string `json:"partPath"`
	FinalPath        string `json:"finalPath"`
	CompressedSize   int64  `json:"compressedSize"`
	UncompressedSize int64  `json:"uncompressedSize"`
	SHA256           string `json:"sha256"`
}

type FinalizeHooks struct {
	// Reserve is called before compressed bytes cross each new reservation
	// extent. The callback must durably reserve the requested bytes.
	Reserve      func(context.Context, int64) error
	BeforeRename func(context.Context, ArtifactMeta) error
	AfterRename  func(context.Context, ArtifactMeta) error
}

// CommitSameVolumeRename exposes the platform-specific durable rename used by
// transfer artifacts. Callers must finish, sync, close and validate the part
// file before invoking it.
func CommitSameVolumeRename(partPath, finalPath string) error {
	partAbs, err := filepath.Abs(partPath)
	if err != nil {
		return err
	}
	finalAbs, err := filepath.Abs(finalPath)
	if err != nil {
		return err
	}
	if filepath.Clean(filepath.Dir(partAbs)) != filepath.Clean(filepath.Dir(finalAbs)) {
		return errors.New("artifact part and final paths must share a directory")
	}
	return durableRename(partAbs, finalAbs)
}

// CommitSameVolumeReplace atomically replaces an existing final file after the
// caller has validated the target and received overwrite confirmation.
func CommitSameVolumeReplace(partPath, finalPath string) error {
	partAbs, err := filepath.Abs(partPath)
	if err != nil {
		return err
	}
	finalAbs, err := filepath.Abs(finalPath)
	if err != nil {
		return err
	}
	if filepath.Clean(filepath.Dir(partAbs)) != filepath.Clean(filepath.Dir(finalAbs)) {
		return errors.New("artifact part and final paths must share a directory")
	}
	return durableReplace(partAbs, finalAbs)
}

type extentWriter struct {
	ctx      context.Context
	w        io.Writer
	reserve  func(context.Context, int64) error
	written  int64
	reserved int64
}

func (w *extentWriter) Write(p []byte) (int, error) {
	needed := w.written + int64(len(p))
	for needed > w.reserved {
		extent := ReservationExtentBytes
		if w.reserve != nil {
			if err := w.reserve(w.ctx, extent); err != nil {
				return 0, err
			}
		}
		w.reserved += extent
	}
	n, err := w.w.Write(p)
	w.written += int64(n)
	return n, err
}

// WriteDurableGzip finalizes the encoder before syncing, validates the entire
// gzip stream and compressed checksum, records FINALIZING through the hook,
// then performs a same-directory durable rename and records COMPLETE.
func WriteDurableGzip(
	ctx context.Context,
	partPath, finalPath string,
	write func(io.Writer) error,
	hooks FinalizeHooks,
) (meta ArtifactMeta, err error) {
	partAbs, err := filepath.Abs(partPath)
	if err != nil {
		return ArtifactMeta{}, err
	}
	finalAbs, err := filepath.Abs(finalPath)
	if err != nil {
		return ArtifactMeta{}, err
	}
	if filepath.Clean(filepath.Dir(partAbs)) != filepath.Clean(filepath.Dir(finalAbs)) {
		return ArtifactMeta{}, errors.New("gzip part and final paths must share a directory")
	}
	if _, err := os.Lstat(finalAbs); err == nil {
		return ArtifactMeta{}, errors.New("final artifact already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return ArtifactMeta{}, err
	}

	file, err := os.OpenFile(partAbs, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return ArtifactMeta{}, err
	}
	renamed := false
	defer func() {
		if file != nil {
			_ = file.Close()
		}
		if err != nil && !renamed {
			_ = os.Remove(partAbs)
		}
	}()

	reservedWriter := &extentWriter{ctx: ctx, w: file, reserve: hooks.Reserve}
	buffered := bufio.NewWriterSize(reservedWriter, 256*1024)
	encoder := gzip.NewWriter(buffered)
	if err = write(encoder); err != nil {
		_ = encoder.Close()
		return ArtifactMeta{}, err
	}
	// Close, not Flush, writes the gzip footer containing CRC32 and ISIZE.
	if err = encoder.Close(); err != nil {
		return ArtifactMeta{}, fmt.Errorf("finalize gzip encoder: %w", err)
	}
	if err = buffered.Flush(); err != nil {
		return ArtifactMeta{}, fmt.Errorf("flush gzip file buffer: %w", err)
	}
	if err = file.Sync(); err != nil {
		return ArtifactMeta{}, fmt.Errorf("sync gzip artifact: %w", err)
	}
	if err = file.Close(); err != nil {
		file = nil
		return ArtifactMeta{}, fmt.Errorf("close gzip artifact: %w", err)
	}
	file = nil

	meta, err = ValidateGzipArtifact(partAbs)
	if err != nil {
		return ArtifactMeta{}, err
	}
	meta.PartPath = partAbs
	meta.FinalPath = finalAbs
	if hooks.BeforeRename != nil {
		if err = hooks.BeforeRename(ctx, meta); err != nil {
			return ArtifactMeta{}, err
		}
	}
	if err = durableRename(partAbs, finalAbs); err != nil {
		return ArtifactMeta{}, fmt.Errorf("commit gzip artifact: %w", err)
	}
	renamed = true
	if hooks.AfterRename != nil {
		if err = hooks.AfterRename(ctx, meta); err != nil {
			return meta, err
		}
	}
	return meta, nil
}

// ValidateGzipArtifact verifies compressed size/SHA-256 and reads exactly one
// gzip member to EOF. Reading to EOF is required for CRC32 and ISIZE checks.
func ValidateGzipArtifact(path string) (ArtifactMeta, error) {
	file, err := os.Open(path)
	if err != nil {
		return ArtifactMeta{}, err
	}
	hash := sha256.New()
	compressed, err := io.Copy(hash, file)
	closeErr := file.Close()
	if err != nil {
		return ArtifactMeta{}, err
	}
	if closeErr != nil {
		return ArtifactMeta{}, closeErr
	}

	file, err = os.Open(path)
	if err != nil {
		return ArtifactMeta{}, err
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	decoder, err := gzip.NewReader(reader)
	if err != nil {
		return ArtifactMeta{}, fmt.Errorf("%w: %v", ErrInvalidGzip, err)
	}
	decoder.Multistream(false)
	uncompressed, copyErr := io.Copy(io.Discard, decoder)
	closeErr = decoder.Close()
	if copyErr != nil {
		return ArtifactMeta{}, fmt.Errorf("%w: %v", ErrInvalidGzip, copyErr)
	}
	if closeErr != nil {
		return ArtifactMeta{}, fmt.Errorf("%w: %v", ErrInvalidGzip, closeErr)
	}
	if _, err := reader.Peek(1); !errors.Is(err, io.EOF) {
		if err == nil {
			return ArtifactMeta{}, fmt.Errorf("%w: extra gzip member or trailing bytes", ErrInvalidGzip)
		}
		return ArtifactMeta{}, err
	}
	return ArtifactMeta{
		CompressedSize: compressed, UncompressedSize: uncompressed,
		SHA256: hex.EncodeToString(hash.Sum(nil)),
	}, nil
}
