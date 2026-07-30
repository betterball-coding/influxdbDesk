package transfer

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"testing"
)

func settleStageReservation(context.Context, int64, int64) error { return nil }

func TestStageImportCanonicalLPAndHashes(t *testing.T) {
	source := []byte("cpu\\ load,z=last,a=first value=09007199254740993i,ok=T 01720000000000000001\n")
	var destination bytes.Buffer
	result, err := StageImport(context.Background(), StageImportRequest{
		Format: ImportStageLP, Source: bytes.NewReader(source), Destination: &destination,
		Reserve: func(context.Context, int64) error { return nil }, SettleReservation: settleStageReservation,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := "cpu\\ load,a=first,z=last ok=true,value=9007199254740993i 1720000000000000001\n"
	if destination.String() != want {
		t.Fatalf("canonical LP\nwant %q\n got %q", want, destination.String())
	}
	sourceDigest := sha256.Sum256(source)
	stagingDigest := sha256.Sum256([]byte(want))
	if result.SourceSHA256 != hex.EncodeToString(sourceDigest[:]) ||
		result.StagingSHA256 != hex.EncodeToString(stagingDigest[:]) ||
		result.LogicalBytes != strconv.Itoa(len(want)) || result.PointCount != "1" {
		t.Fatalf("result=%+v", result)
	}
}

func TestStageImportRejectsMissingTimestamp(t *testing.T) {
	_, err := StageImport(context.Background(), StageImportRequest{
		Format: ImportStageTXT, Source: strings.NewReader("m value=1i\n"), Destination: &bytes.Buffer{},
		Reserve: func(context.Context, int64) error { return nil }, SettleReservation: settleStageReservation,
	})
	if !errors.Is(err, ErrStageMissingTimestamp) {
		t.Fatalf("missing timestamp error=%v", err)
	}
}

func TestStageImportTypedJSONLPreservesExactNumbers(t *testing.T) {
	source := `{"schemaVersion":1,"measurement":"cpu load","tags":{"z":"last","a":"first"},` +
		`"fields":{"unsigned":{"kind":"uint64","decimalText":"18446744073709551615"},` +
		`"signed":{"kind":"int64","decimalText":"9007199254740993"},` +
		`"negativeZero":{"kind":"float64","decimalText":"-0"}},` +
		`"timestamp":{"kind":"timestamp_ns","decimalText":"1720000000000000001"}}` + "\n"
	var destination bytes.Buffer
	result, err := StageImport(context.Background(), StageImportRequest{
		Format: ImportStageTypedJSONL, Source: strings.NewReader(source), Destination: &destination,
		Reserve: func(context.Context, int64) error { return nil }, SettleReservation: settleStageReservation,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := "cpu\\ load,a=first,z=last negativeZero=-0,signed=9007199254740993i,unsigned=18446744073709551615u 1720000000000000001\n"
	if destination.String() != want {
		t.Fatalf("typed JSONL canonical LP\nwant %q\n got %q", want, destination.String())
	}
	if result.PointCount != "1" || result.LogicalBytes != strconv.Itoa(len(want)) {
		t.Fatalf("result=%+v", result)
	}
}

func TestStageImportTypedJSONLRejectsNumericTextWithoutMapping(t *testing.T) {
	source := `{"schemaVersion":1,"measurement":"m","fields":` +
		`{"value":{"kind":"numeric_text","decimalText":"1"}},` +
		`"timestamp":{"kind":"timestamp_ns","decimalText":"1"}}` + "\n"
	_, err := StageImport(context.Background(), StageImportRequest{
		Format: ImportStageTypedJSONL, Source: strings.NewReader(source), Destination: &bytes.Buffer{},
		Reserve: func(context.Context, int64) error { return nil }, SettleReservation: settleStageReservation,
	})
	if !errors.Is(err, ErrStageAmbiguousNumeric) {
		t.Fatalf("numeric_text error=%v", err)
	}
}

func TestStageImportRejectsTagFieldKeyCollision(t *testing.T) {
	tests := []StageImportRequest{
		{
			Format: ImportStageLP, Source: strings.NewReader("m,shared=tag shared=1i 1\n"),
			Destination: &bytes.Buffer{}, Reserve: func(context.Context, int64) error { return nil },
			SettleReservation: settleStageReservation,
		},
		{
			Format: ImportStageTypedJSONL,
			Source: strings.NewReader(`{"schemaVersion":1,"measurement":"m","tags":{"shared":"tag"},` +
				`"fields":{"shared":{"kind":"int64","decimalText":"1"}},` +
				`"timestamp":{"kind":"timestamp_ns","decimalText":"1"}}` + "\n"),
			Destination: &bytes.Buffer{}, Reserve: func(context.Context, int64) error { return nil },
			SettleReservation: settleStageReservation,
		},
	}
	for _, request := range tests {
		if _, err := StageImport(context.Background(), request); !errors.Is(err, ErrStageInvalidInput) {
			t.Fatalf("tag/field collision error=%v", err)
		}
	}
}

func TestStageImportGzipIntegrityAndLimit(t *testing.T) {
	point := []byte("m value=1i 1\n")
	t.Run("valid", func(t *testing.T) {
		compressed := gzipStageBytes(t, point)
		var destination bytes.Buffer
		result, err := StageImport(context.Background(), StageImportRequest{
			Format: ImportStageLPGzip, Source: bytes.NewReader(compressed), Destination: &destination,
			Reserve: func(context.Context, int64) error { return nil }, SettleReservation: settleStageReservation,
		})
		if err != nil || destination.String() != string(point) || result.PointCount != "1" {
			t.Fatalf("result=%+v output=%q err=%v", result, destination.String(), err)
		}
		sourceDigest := sha256.Sum256(compressed)
		if result.SourceSHA256 != hex.EncodeToString(sourceDigest[:]) {
			t.Fatalf("source digest=%s", result.SourceSHA256)
		}
	})

	t.Run("crc", func(t *testing.T) {
		compressed := gzipStageBytes(t, point)
		compressed[len(compressed)-8] ^= 0xff
		_, err := StageImport(context.Background(), StageImportRequest{
			Format: ImportStageLPGzip, Source: bytes.NewReader(compressed), Destination: &bytes.Buffer{},
			Reserve: func(context.Context, int64) error { return nil }, SettleReservation: settleStageReservation,
		})
		if !errors.Is(err, ErrStageInvalidGzip) {
			t.Fatalf("CRC error=%v", err)
		}
	})

	t.Run("extra member", func(t *testing.T) {
		compressed := append(gzipStageBytes(t, point), gzipStageBytes(t, point)...)
		_, err := StageImport(context.Background(), StageImportRequest{
			Format: ImportStageLPGzip, Source: bytes.NewReader(compressed), Destination: &bytes.Buffer{},
			Reserve: func(context.Context, int64) error { return nil }, SettleReservation: settleStageReservation,
		})
		if !errors.Is(err, ErrStageInvalidGzip) {
			t.Fatalf("extra member error=%v", err)
		}
	})

	t.Run("bomb limit", func(t *testing.T) {
		uncompressed := bytes.Repeat(point, 32)
		compressed := gzipStageBytes(t, uncompressed)
		_, err := StageImport(context.Background(), StageImportRequest{
			Format: ImportStageLPGzip, Source: bytes.NewReader(compressed), Destination: &bytes.Buffer{},
			Reserve:                    func(context.Context, int64) error { return nil },
			SettleReservation:          settleStageReservation,
			GzipUncompressedLimitBytes: int64(len(point) * 2),
		})
		if !errors.Is(err, ErrStageGzipLimit) {
			t.Fatalf("bomb limit error=%v", err)
		}
	})
}

func TestStageImportCanonicalPointFiveMiBBoundary(t *testing.T) {
	overhead := len(`m value="" 1`)
	value := strings.Repeat("x", int(StageCanonicalPointLimitBytes)-overhead)
	atLimit := `m value="` + value + `" 1` + "\n"
	var destination bytes.Buffer
	result, err := StageImport(context.Background(), StageImportRequest{
		Format: ImportStageLP, Source: strings.NewReader(atLimit), Destination: &destination,
		Reserve: func(context.Context, int64) error { return nil }, SettleReservation: settleStageReservation,
	})
	if err != nil {
		t.Fatal(err)
	}
	if destination.Len() != int(StageCanonicalPointLimitBytes)+1 ||
		result.LogicalBytes != strconv.FormatInt(StageCanonicalPointLimitBytes+1, 10) {
		t.Fatalf("boundary output=%d result=%+v", destination.Len(), result)
	}

	overLimit := `m value="` + value + `x" 1` + "\n"
	_, err = StageImport(context.Background(), StageImportRequest{
		Format: ImportStageLP, Source: strings.NewReader(overLimit), Destination: &bytes.Buffer{},
		Reserve: func(context.Context, int64) error { return nil }, SettleReservation: settleStageReservation,
	})
	if !errors.Is(err, ErrStagePointTooLarge) {
		t.Fatalf("over-limit error=%v", err)
	}
}

func TestStageImportReservesBeforeWriting(t *testing.T) {
	audit := &stageReservationAudit{extent: 8}
	result, err := StageImport(context.Background(), StageImportRequest{
		Format: ImportStageLP, Source: strings.NewReader("measurement value=1i 1\n"),
		Destination: audit, ReservationUnitBytes: audit.extent,
		Reserve: func(_ context.Context, bytes int64) error {
			audit.events = append(audit.events, "reserve:"+strconv.FormatInt(bytes, 10))
			audit.reserved += bytes
			return nil
		},
		SettleReservation: func(_ context.Context, reserved, written int64) error {
			audit.events = append(audit.events, "settle")
			audit.settledReserved = reserved
			audit.settledWritten = written
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if audit.violation || len(audit.events) < 3 || !strings.HasPrefix(audit.events[0], "reserve:") ||
		audit.events[len(audit.events)-1] != "settle" {
		t.Fatalf("reservation events=%v violation=%v", audit.events, audit.violation)
	}
	logical, _ := strconv.ParseInt(result.LogicalBytes, 10, 64)
	if audit.written != logical || audit.reserved < audit.written ||
		audit.settledReserved != audit.reserved || audit.settledWritten != audit.written ||
		result.ReservedBytes != strconv.FormatInt(audit.reserved, 10) {
		t.Fatalf("reserved=%d written=%d result=%+v", audit.reserved, audit.written, result)
	}
}

func TestStageImportMapsUnderlyingErrorsToSafeCodes(t *testing.T) {
	secret := "do-not-expose-source-or-path"
	settlements := 0
	_, err := StageImport(context.Background(), StageImportRequest{
		Format: ImportStageLP, Source: strings.NewReader("m value=1i 1\n"), Destination: &bytes.Buffer{},
		Reserve: func(context.Context, int64) error { return errors.New(secret) },
		SettleReservation: func(context.Context, int64, int64) error {
			settlements++
			return nil
		},
	})
	if !errors.Is(err, ErrStageReservation) || strings.Contains(err.Error(), secret) || settlements != 0 {
		t.Fatalf("reservation error=%q", err)
	}
	_, err = StageImport(context.Background(), StageImportRequest{
		Format: ImportStageLP, Source: strings.NewReader("m value=1i 1\n"),
		Destination: stageWriterFunc(func([]byte) (int, error) { return 0, errors.New(secret) }),
		Reserve:     func(context.Context, int64) error { return nil }, SettleReservation: settleStageReservation,
	})
	if !errors.Is(err, ErrStageWrite) || strings.Contains(err.Error(), secret) {
		t.Fatalf("write error=%q", err)
	}
	_, err = StageImport(context.Background(), StageImportRequest{
		Format: ImportStageLP, Source: strings.NewReader("m value=1i 1\n"), Destination: &bytes.Buffer{},
		Reserve: func(context.Context, int64) error { return nil },
		SettleReservation: func(context.Context, int64, int64) error {
			return errors.New(secret)
		},
	})
	if !errors.Is(err, ErrStageSettlement) || strings.Contains(err.Error(), secret) {
		t.Fatalf("settlement error=%q", err)
	}
}

type stageReservationAudit struct {
	extent          int64
	reserved        int64
	written         int64
	violation       bool
	events          []string
	settledReserved int64
	settledWritten  int64
}

type stageWriterFunc func([]byte) (int, error)

func (write stageWriterFunc) Write(payload []byte) (int, error) { return write(payload) }

func (w *stageReservationAudit) Write(payload []byte) (int, error) {
	w.events = append(w.events, "write:"+strconv.Itoa(len(payload)))
	if w.written+int64(len(payload)) > w.reserved {
		w.violation = true
	}
	w.written += int64(len(payload))
	return len(payload), nil
}

func gzipStageBytes(t *testing.T, input []byte) []byte {
	t.Helper()
	var output bytes.Buffer
	writer := gzip.NewWriter(&output)
	if _, err := writer.Write(input); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}
