package transfer

// ImportTargetDigest is the sole canonical digest for an Import /write target.
// Both preflight authorization and the network worker use it so a caller
// cannot pair a valid grant digest with a different database or RP.
func ImportTargetDigest(target ImportTarget) (string, error) {
	if !validPreflightText(target.Database) ||
		(target.RetentionPolicy != "" && !validPreflightText(target.RetentionPolicy)) {
		return "", ErrPreflightInvalid
	}
	return digestJSON(struct {
		SchemaVersion   int    `json:"schemaVersion"`
		Database        string `json:"database"`
		RetentionPolicy string `json:"retentionPolicy"`
	}{1, target.Database, target.RetentionPolicy})
}
