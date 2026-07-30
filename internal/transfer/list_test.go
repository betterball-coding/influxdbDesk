package transfer

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestListRecoverableImportsFiltersProfileAndTerminalJobs(t *testing.T) {
	repository, database := newTransferRepository(t)
	defer database.Close()
	for _, profileID := range []string{"profile-a", "profile-b"} {
		if _, _, err := repository.CreateImport(context.Background(), CreateImportRequest{
			JobID: uuid.NewString(), ProfileID: profileID,
			ClientScope: "scope-" + profileID, RequestDigest: "digest-" + profileID,
			LedgerExpiresAt: time.Now().Add(time.Hour), InitialCheckpoint: initialCheckpoint(),
		}); err != nil {
			t.Fatal(err)
		}
	}
	jobs, err := repository.ListRecoverableImports(context.Background(), "profile-a", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].ProfileID != "profile-a" || jobs[0].Task.State != ImportReady {
		t.Fatalf("recoverable jobs=%+v", jobs)
	}
}
