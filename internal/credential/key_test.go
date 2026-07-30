package credential

import "testing"

func TestKeyIsCanonicalAndScoped(t *testing.T) {
	key, err := Key("550E8400-E29B-41D4-A716-446655440000", KindJWT)
	if err != nil {
		t.Fatal(err)
	}
	if key != "InfluxDesk/550e8400-e29b-41d4-a716-446655440000/jwt" {
		t.Fatalf("unexpected key %q", key)
	}
	if _, err := Key("not-a-uuid", KindJWT); err != ErrInvalidReference {
		t.Fatalf("expected invalid UUID error, got %v", err)
	}
	if _, err := Key("550e8400-e29b-41d4-a716-446655440000", Kind("other")); err != ErrInvalidReference {
		t.Fatalf("expected invalid kind error, got %v", err)
	}
}
