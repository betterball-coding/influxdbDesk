package transfer

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
)

func CheckpointDigest(checkpoint Checkpoint) (string, error) {
	if _, err := canonicalDecimal(checkpoint.Sequence); err != nil {
		return "", err
	}
	if _, err := canonicalDecimal(checkpoint.LogicalOffset); err != nil {
		return "", err
	}
	if _, err := canonicalDecimal(checkpoint.AdaptiveMaxPoints); err != nil {
		return "", err
	}
	if _, err := canonicalDecimal(checkpoint.AdaptiveMaxBytes); err != nil {
		return "", err
	}
	if checkpoint.SourceSHA256 == "" || checkpoint.StagingSHA256 == "" ||
		checkpoint.NormalizationVersion == "" || checkpoint.SpecDigest == "" || checkpoint.TargetDigest == "" {
		return "", errors.New("checkpoint identity fields are required")
	}
	encoded, err := json.Marshal(struct {
		SchemaVersion int        `json:"schemaVersion"`
		Checkpoint    Checkpoint `json:"checkpoint"`
	}{SchemaVersion: 1, Checkpoint: checkpoint})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}
