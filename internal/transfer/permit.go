package transfer

import "sync"

type PermitRegistry struct {
	mu      sync.Mutex
	permits map[string]Permit
}

func NewPermitRegistry() *PermitRegistry {
	return &PermitRegistry{permits: make(map[string]Permit)}
}

func (r *PermitRegistry) Activate(jobID string, permit Permit) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !validPermit(permit) {
		return ErrPermitMismatch
	}
	if _, exists := r.permits[jobID]; exists {
		return ErrPermitMismatch
	}
	r.permits[jobID] = permit
	return nil
}

func (r *PermitRegistry) Get(jobID string) (Permit, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.permits[jobID]
	return p, ok
}

func (r *PermitRegistry) Validate(jobID string, expected Permit) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.permits[jobID]
	if !ok || p != expected || !validPermit(p) {
		return ErrPermitMismatch
	}
	return nil
}

func (r *PermitRegistry) CAS(jobID, runSegmentID, parentDigest, childDigest string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.permits[jobID]
	if !ok || p.RunSegmentID != runSegmentID || p.CurrentCheckpointDigest != parentDigest {
		return false
	}
	p.CurrentCheckpointDigest = childDigest
	r.permits[jobID] = p
	return true
}

func (r *PermitRegistry) Revoke(jobID string) {
	r.mu.Lock()
	delete(r.permits, jobID)
	r.mu.Unlock()
}

func (r *PermitRegistry) RevokeIfMatch(jobID string, expected Permit) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if current, ok := r.permits[jobID]; !ok || current != expected {
		return false
	}
	delete(r.permits, jobID)
	return true
}

type permitEntry struct {
	jobID  string
	permit Permit
}

func (r *PermitRegistry) Matching(connectionID, generation, protectionRevision string) []permitEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	entries := make([]permitEntry, 0)
	for jobID, permit := range r.permits {
		if permit.ConnectionID == connectionID && permit.ConnectionGeneration == generation &&
			permit.ProtectionRevision == protectionRevision {
			entries = append(entries, permitEntry{jobID: jobID, permit: permit})
		}
	}
	return entries
}

func (r *PermitRegistry) Reset() {
	r.mu.Lock()
	r.permits = make(map[string]Permit)
	r.mu.Unlock()
}

func validPermit(permit Permit) bool {
	if permit.RunSegmentID == "" || permit.CurrentCheckpointDigest == "" || permit.ProfileID == "" ||
		permit.ProfileRevision == "" || permit.ConnectionID == "" || permit.LeaseID == "" {
		return false
	}
	if _, err := canonicalDecimal(permit.ProfileRevision); err != nil {
		return false
	}
	if _, err := canonicalDecimal(permit.ConnectionGeneration); err != nil {
		return false
	}
	if _, err := canonicalDecimal(permit.ProtectionRevision); err != nil {
		return false
	}
	return true
}
