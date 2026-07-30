package exportjob

import (
	"context"
	"errors"
	"strconv"

	"github.com/influxdesk/influxdesk/internal/transfer"
)

// GzipFileValidator is the production validator for reusable DATA fragments.
// transfer.ValidateGzipArtifact hashes the complete compressed file and reads
// exactly one gzip member to EOF, which verifies CRC32 and ISIZE and rejects
// extra members or trailing bytes.
type GzipFileValidator struct{}

func (GzipFileValidator) ValidateFragment(ctx context.Context, fragment Fragment) (ArtifactValidation, error) {
	if err := ctx.Err(); err != nil {
		return ArtifactValidation{}, err
	}
	if fragment.Kind != FragmentData || fragment.FinalPath == nil {
		return ArtifactValidation{}, errors.New("gzip data fragment final path is unavailable")
	}
	meta, err := transfer.ValidateGzipArtifact(*fragment.FinalPath)
	if err != nil {
		return ArtifactValidation{}, err
	}
	if err := ctx.Err(); err != nil {
		return ArtifactValidation{}, err
	}
	return ArtifactValidation{
		CompressedSize:   strconv.FormatInt(meta.CompressedSize, 10),
		UncompressedSize: strconv.FormatInt(meta.UncompressedSize, 10),
		ChecksumSHA256:   meta.SHA256,
	}, nil
}
