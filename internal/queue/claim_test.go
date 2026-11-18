package queue

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

// This stub checks the Go call contract without claiming to simulate PG locks.
// Real races and recovery are covered by tests/claim_integration.sh on PostgreSQL.
type claimDriver struct{}
type claimConn struct{ mode string }
type claimRows struct {
	mode string
	read bool
}

func (claimDriver) Open(name string) (driver.Conn, error) { return &claimConn{mode: name}, nil }
func (*claimConn) Prepare(string) (driver.Stmt, error)    { return nil, errors.New("unused") }
func (*claimConn) Close() error                           { return nil }
func (*claimConn) Begin() (driver.Tx, error)              { return nil, errors.New("unused") }
func (c *claimConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if !strings.Contains(query, "queue_claim(") || len(args) != 2 || args[0].Value != "worker-A" {
		return nil, errors.New("unexpected claim query or arguments")
	}
	seconds, ok := args[1].Value.(float64)
	if !ok || seconds != 30 {
		return nil, errors.New("incorrect lease duration")
	}
	if c.mode == "error" {
		return nil, errors.New("database unavailable")
	}
	return &claimRows{mode: c.mode}, nil
}
func (*claimRows) Columns() []string {
	return []string{"job_id", "job_type", "payload", "attempt_no", "claim_token", "lease_until"}
}
func (*claimRows) Close() error { return nil }
func (r *claimRows) Next(dest []driver.Value) error {
	if r.mode == "none" || r.read {
		return io.EOF
	}
	r.read = true
	dest[0], dest[1], dest[2] = int64(42), "example", []byte(`{"key":"value"}`)
	dest[3], dest[4], dest[5] = int64(2), "token-2", time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)
	return nil
}

func init() { sql.Register("queue-claim-stub", claimDriver{}) }

func TestClaimNext(t *testing.T) {
	for _, tc := range []struct {
		mode       string
		hasClaim   bool
		wantsError bool
	}{
		{"sample", true, false}, {"none", false, false}, {"error", false, true},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			db, err := sql.Open("queue-claim-stub", tc.mode)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			claim, err := ClaimNext(context.Background(), db, "worker-A", 30*time.Second)
			if (err != nil) != tc.wantsError {
				t.Fatalf("error: %v", err)
			}
			if (claim != nil) != tc.hasClaim {
				t.Fatalf("claim: %+v", claim)
			}
			if claim != nil && (claim.JobID != 42 || claim.Attempt != 2 || claim.Token != "token-2" || string(claim.Payload) != `{"key":"value"}`) {
				t.Fatalf("unexpected claim: %+v", claim)
			}
		})
	}
}

func TestClaimRejectsInvalidInput(t *testing.T) {
	for _, tc := range []struct {
		worker string
		lease  time.Duration
	}{
		{"", time.Second}, {"  ", time.Second}, {"worker-A", 0}, {"worker-A", time.Microsecond},
	} {
		if _, err := ClaimNext(context.Background(), nil, tc.worker, tc.lease); err == nil {
			t.Errorf("accepted worker=%q lease=%v", tc.worker, tc.lease)
		}
	}
}
