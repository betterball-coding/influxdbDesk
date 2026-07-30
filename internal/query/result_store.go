package query

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
)

const (
	defaultResultMemoryLimitBytes = uint64(32 << 20)
	defaultResultMaxBytes         = uint64(128 << 20)
	defaultResultMaxRows          = uint64(100_000)
	spillNonceSize                = 12
)

var (
	rowEncodingMagic = [4]byte{'I', 'D', 'R', 1}
	spillFileMagic   = [8]byte{'I', 'D', 'Q', 'S', 'P', 'L', 1, '\n'}
	errStoreClosed   = errors.New("query result store is closed")
)

type resultStoreOptions struct {
	directory   string
	memoryBytes uint64
	maxBytes    uint64
	maxRows     uint64
}

type resultStore struct {
	sessionID string
	options   resultStoreOptions
	aead      cipher.AEAD
	key       [32]byte

	rows        map[resultSeriesKey][]*storedRow
	memoryRows  []*storedRow
	totalRows   uint64
	totalBytes  uint64
	nextOrdinal uint64

	spill     *os.File
	spillPath string
	closed    bool
}

type resultSeriesKey struct {
	statementID int
	seriesID    string
}

type storedRow struct {
	ordinal     uint64
	memory      []byte
	frameOffset int64
	frameLength uint32
}

func newResultStore(sessionID string, options resultStoreOptions) (*resultStore, error) {
	if sessionID == "" {
		return nil, errors.New("query result session ID is required")
	}
	if options.memoryBytes == 0 {
		options.memoryBytes = defaultResultMemoryLimitBytes
	}
	if options.maxBytes == 0 {
		options.maxBytes = defaultResultMaxBytes
	}
	if options.maxRows == 0 {
		options.maxRows = defaultResultMaxRows
	}
	store := &resultStore{
		sessionID: sessionID,
		options:   options,
		rows:      make(map[resultSeriesKey][]*storedRow),
	}
	if _, err := io.ReadFull(rand.Reader, store.key[:]); err != nil {
		return nil, fmt.Errorf("create query result key: %w", err)
	}
	block, err := aes.NewCipher(store.key[:])
	if err != nil {
		return nil, fmt.Errorf("create query result cipher: %w", err)
	}
	store.aead, err = cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create query result GCM: %w", err)
	}
	return store, nil
}

func (s *resultStore) AppendDecodedRow(statementID int, seriesID string, row []TypedScalar) (bool, bool, error) {
	if s == nil || s.closed {
		return false, false, errStoreClosed
	}
	payload, err := encodeTypedRowV1(row)
	if err != nil {
		return false, false, err
	}
	payloadBytes := uint64(len(payload))
	if s.totalRows >= s.options.maxRows || payloadBytes > s.options.maxBytes-s.totalBytes {
		return false, true, nil
	}

	ref := &storedRow{ordinal: s.nextOrdinal}
	if s.spill != nil || s.totalBytes+payloadBytes >= s.options.memoryBytes {
		if err := s.ensureSpill(); err != nil {
			return false, false, err
		}
		if err := s.writeSpillRow(ref, payload); err != nil {
			return false, false, err
		}
	} else {
		ref.memory = payload
		s.memoryRows = append(s.memoryRows, ref)
	}

	key := resultSeriesKey{statementID: statementID, seriesID: seriesID}
	s.rows[key] = append(s.rows[key], ref)
	s.totalRows++
	s.totalBytes += payloadBytes
	s.nextOrdinal++
	return true, s.totalRows >= s.options.maxRows || s.totalBytes >= s.options.maxBytes, nil
}

func (s *resultStore) RowCount(statementID int, seriesID string) uint64 {
	if s == nil || s.closed {
		return 0
	}
	return uint64(len(s.rows[resultSeriesKey{statementID: statementID, seriesID: seriesID}]))
}

func (s *resultStore) TotalRows() uint64 {
	if s == nil || s.closed {
		return 0
	}
	return s.totalRows
}

func (s *resultStore) ReadRows(statementID int, seriesID string, offset uint64, limit int) ([][]TypedScalar, uint64, error) {
	if s == nil || s.closed {
		return nil, 0, errStoreClosed
	}
	refs, ok := s.rows[resultSeriesKey{statementID: statementID, seriesID: seriesID}]
	if !ok || offset > uint64(len(refs)) {
		return nil, 0, errors.New("query result series or row offset does not exist")
	}
	end := offset + uint64(limit)
	if end > uint64(len(refs)) {
		end = uint64(len(refs))
	}
	rows := make([][]TypedScalar, 0, end-offset)
	for _, ref := range refs[offset:end] {
		payload, err := s.readStoredRow(ref)
		if err != nil {
			return nil, uint64(len(refs)), err
		}
		row, err := decodeTypedRowV1(payload)
		if err != nil {
			return nil, uint64(len(refs)), fmt.Errorf("decode retained query row: %w", err)
		}
		rows = append(rows, row)
	}
	return rows, uint64(len(refs)), nil
}

func (s *resultStore) ensureSpill() error {
	if s.spill != nil {
		return nil
	}
	if s.options.directory == "" {
		return errors.New("query spill directory is not configured")
	}
	if err := ensurePrivateSpillDirectory(s.options.directory); err != nil {
		return err
	}
	file, err := os.CreateTemp(s.options.directory, ".query-"+s.sessionID+"-*.spill")
	if err != nil {
		return fmt.Errorf("create query spill: %w", err)
	}
	cleanup := func() {
		_ = file.Close()
		_ = os.Remove(file.Name())
	}
	if err := file.Chmod(0o600); err != nil {
		cleanup()
		return fmt.Errorf("protect query spill: %w", err)
	}
	if err := writeAll(file, spillFileMagic[:]); err != nil {
		cleanup()
		return fmt.Errorf("write query spill header: %w", err)
	}
	s.spill = file
	s.spillPath = file.Name()
	for _, ref := range s.memoryRows {
		if err := s.writeSpillRow(ref, ref.memory); err != nil {
			return err
		}
		ref.memory = nil
	}
	s.memoryRows = nil
	return nil
}

func ensurePrivateSpillDirectory(directory string) error {
	directory = filepath.Clean(directory)
	info, err := os.Lstat(directory)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return fmt.Errorf("create query spill directory: %w", err)
		}
		info, err = os.Lstat(directory)
	}
	if err != nil {
		return fmt.Errorf("inspect query spill directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return errors.New("query spill path is not a private directory")
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return fmt.Errorf("protect query spill directory: %w", err)
	}
	return nil
}

func (s *resultStore) writeSpillRow(ref *storedRow, payload []byte) error {
	if s.spill == nil {
		return errors.New("query spill is not open")
	}
	nonce := make([]byte, spillNonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return fmt.Errorf("create query spill nonce: %w", err)
	}
	ciphertext := s.aead.Seal(nil, nonce, payload, s.rowAAD(ref.ordinal))
	if len(ciphertext) > math.MaxUint32 {
		return errors.New("query spill row is too large")
	}
	offset, err := s.spill.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("locate query spill row: %w", err)
	}
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(ciphertext)))
	if err := writeAll(s.spill, length[:]); err != nil {
		return fmt.Errorf("write query spill length: %w", err)
	}
	if err := writeAll(s.spill, nonce); err != nil {
		return fmt.Errorf("write query spill nonce: %w", err)
	}
	if err := writeAll(s.spill, ciphertext); err != nil {
		return fmt.Errorf("write query spill ciphertext: %w", err)
	}
	ref.frameOffset = offset
	ref.frameLength = uint32(4 + len(nonce) + len(ciphertext))
	return nil
}

func (s *resultStore) readStoredRow(ref *storedRow) ([]byte, error) {
	if ref.memory != nil {
		return bytes.Clone(ref.memory), nil
	}
	if s.spill == nil || ref.frameLength < 4+spillNonceSize {
		return nil, errors.New("query spill row reference is invalid")
	}
	frame := make([]byte, int(ref.frameLength))
	if _, err := s.spill.ReadAt(frame, ref.frameOffset); err != nil {
		return nil, fmt.Errorf("read query spill row: %w", err)
	}
	ciphertextLength := binary.BigEndian.Uint32(frame[:4])
	if int(ciphertextLength) != len(frame)-4-spillNonceSize {
		return nil, errors.New("query spill row length is invalid")
	}
	nonce := frame[4 : 4+spillNonceSize]
	ciphertext := frame[4+spillNonceSize:]
	payload, err := s.aead.Open(nil, nonce, ciphertext, s.rowAAD(ref.ordinal))
	if err != nil {
		return nil, errors.New("query spill authentication failed")
	}
	return payload, nil
}

func (s *resultStore) rowAAD(ordinal uint64) []byte {
	buffer := make([]byte, 1+4+len(s.sessionID)+8)
	buffer[0] = 1
	binary.BigEndian.PutUint32(buffer[1:5], uint32(len(s.sessionID)))
	copy(buffer[5:], s.sessionID)
	binary.BigEndian.PutUint64(buffer[len(buffer)-8:], ordinal)
	return buffer
}

func (s *resultStore) Close() error {
	if s == nil {
		return nil
	}
	s.closed = true
	var closeErr error
	if s.spill != nil {
		closeErr = s.spill.Close()
		s.spill = nil
	}
	var removeErr error
	if s.spillPath != "" {
		removeErr = os.Remove(s.spillPath)
		if errors.Is(removeErr, os.ErrNotExist) {
			removeErr = nil
		}
		if removeErr == nil {
			s.spillPath = ""
		}
	}
	for i := range s.key {
		s.key[i] = 0
	}
	s.aead = nil
	s.rows = nil
	s.memoryRows = nil
	s.totalRows = 0
	s.totalBytes = 0
	return errors.Join(closeErr, removeErr)
}

func encodeTypedRowV1(row []TypedScalar) ([]byte, error) {
	if len(row) > math.MaxUint32 {
		return nil, errors.New("query row has too many columns")
	}
	var buffer bytes.Buffer
	buffer.Write(rowEncodingMagic[:])
	var count [4]byte
	binary.BigEndian.PutUint32(count[:], uint32(len(row)))
	buffer.Write(count[:])
	for _, scalar := range row {
		code, payload, err := encodeScalarV1(scalar)
		if err != nil {
			return nil, err
		}
		buffer.WriteByte(code)
		switch code {
		case 3:
			buffer.Write(payload)
		case 2, 4, 5, 6, 7, 8:
			if len(payload) > math.MaxUint32 {
				return nil, errors.New("query scalar payload is too large")
			}
			var length [4]byte
			binary.BigEndian.PutUint32(length[:], uint32(len(payload)))
			buffer.Write(length[:])
			buffer.Write(payload)
		}
	}
	return buffer.Bytes(), nil
}

func encodeScalarV1(scalar TypedScalar) (byte, []byte, error) {
	switch scalar.Kind {
	case ScalarNull:
		if scalar.DecimalText != "" || scalar.StringValue != "" || scalar.BooleanValue != nil {
			return 0, nil, errors.New("null query scalar contains an unexpected value")
		}
		return 1, nil, nil
	case ScalarString:
		if scalar.DecimalText != "" || scalar.BooleanValue != nil {
			return 0, nil, errors.New("string query scalar contains an unexpected value")
		}
		return 2, []byte(scalar.StringValue), nil
	case ScalarBoolean:
		if scalar.BooleanValue == nil || scalar.DecimalText != "" || scalar.StringValue != "" {
			return 0, nil, errors.New("boolean query scalar has no value")
		}
		if *scalar.BooleanValue {
			return 3, []byte{1}, nil
		}
		return 3, []byte{0}, nil
	case ScalarTimestampNS:
		if err := validateNumericScalarFields(scalar); err != nil {
			return 0, nil, err
		}
		return 4, []byte(scalar.DecimalText), nil
	case ScalarInt64:
		if err := validateNumericScalarFields(scalar); err != nil {
			return 0, nil, err
		}
		return 5, []byte(scalar.DecimalText), nil
	case ScalarUint64:
		if err := validateNumericScalarFields(scalar); err != nil {
			return 0, nil, err
		}
		return 6, []byte(scalar.DecimalText), nil
	case ScalarFloat64:
		if err := validateNumericScalarFields(scalar); err != nil {
			return 0, nil, err
		}
		return 7, []byte(scalar.DecimalText), nil
	case ScalarNumericText:
		if scalar.StringValue != "" || scalar.BooleanValue != nil || !isJSONNumberToken(scalar.DecimalText) {
			return 0, nil, errors.New("numeric_text query scalar is not a JSON number")
		}
		return 8, []byte(scalar.DecimalText), nil
	default:
		return 0, nil, fmt.Errorf("unknown query scalar kind %q", scalar.Kind)
	}
}

func validateNumericScalarFields(scalar TypedScalar) error {
	if scalar.StringValue != "" || scalar.BooleanValue != nil {
		return fmt.Errorf("query scalar %s contains an unexpected value", scalar.Kind)
	}
	return validateNumericScalar(scalar)
}

func validateNumericScalar(scalar TypedScalar) error {
	validated, err := numericScalar(scalar.DecimalText, scalar.Kind)
	if err != nil || validated.DecimalText != scalar.DecimalText {
		return fmt.Errorf("query scalar %s is not canonical", scalar.Kind)
	}
	return nil
}

func isJSONNumberToken(value string) bool {
	decoder := json.NewDecoder(bytes.NewBufferString(value))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return false
	}
	if _, ok := decoded.(json.Number); !ok {
		return false
	}
	return decoder.Decode(&struct{}{}) == io.EOF
}

func decodeTypedRowV1(payload []byte) ([]TypedScalar, error) {
	if len(payload) < len(rowEncodingMagic)+4 || !bytes.Equal(payload[:len(rowEncodingMagic)], rowEncodingMagic[:]) {
		return nil, errors.New("query row encoding version is invalid")
	}
	offset := len(rowEncodingMagic)
	count := binary.BigEndian.Uint32(payload[offset : offset+4])
	offset += 4
	row := make([]TypedScalar, 0, count)
	for range count {
		if offset >= len(payload) {
			return nil, io.ErrUnexpectedEOF
		}
		code := payload[offset]
		offset++
		var scalar TypedScalar
		switch code {
		case 1:
			scalar.Kind = ScalarNull
		case 3:
			if offset >= len(payload) || payload[offset] > 1 {
				return nil, errors.New("query boolean scalar is invalid")
			}
			value := payload[offset] == 1
			offset++
			scalar = TypedScalar{Kind: ScalarBoolean, BooleanValue: &value}
		case 2, 4, 5, 6, 7, 8:
			value, next, err := readLengthPrefixed(payload, offset)
			if err != nil {
				return nil, err
			}
			offset = next
			switch code {
			case 2:
				scalar = TypedScalar{Kind: ScalarString, StringValue: string(value)}
			case 4:
				scalar = TypedScalar{Kind: ScalarTimestampNS, DecimalText: string(value)}
			case 5:
				scalar = TypedScalar{Kind: ScalarInt64, DecimalText: string(value)}
			case 6:
				scalar = TypedScalar{Kind: ScalarUint64, DecimalText: string(value)}
			case 7:
				scalar = TypedScalar{Kind: ScalarFloat64, DecimalText: string(value)}
			case 8:
				scalar = TypedScalar{Kind: ScalarNumericText, DecimalText: string(value)}
			}
			if code != 2 {
				encodedCode, _, err := encodeScalarV1(scalar)
				if err != nil || encodedCode != code {
					return nil, errors.New("query numeric scalar encoding is invalid")
				}
			}
		default:
			return nil, errors.New("query scalar encoding kind is invalid")
		}
		row = append(row, scalar)
	}
	if offset != len(payload) {
		return nil, errors.New("query row encoding has trailing data")
	}
	return row, nil
}

func readLengthPrefixed(payload []byte, offset int) ([]byte, int, error) {
	if len(payload)-offset < 4 {
		return nil, offset, io.ErrUnexpectedEOF
	}
	length := uint64(binary.BigEndian.Uint32(payload[offset : offset+4]))
	offset += 4
	if length > uint64(len(payload)-offset) {
		return nil, offset, io.ErrUnexpectedEOF
	}
	end := offset + int(length)
	return payload[offset:end], end, nil
}

func writeAll(writer io.Writer, payload []byte) error {
	for len(payload) > 0 {
		written, err := writer.Write(payload)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		payload = payload[written:]
	}
	return nil
}
