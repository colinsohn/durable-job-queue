package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"
)

func TestLeaseValidation(t *testing.T) {
	db, err := sql.Open("queue-claim-stub", "unused")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	token := "00000000-0000-0000-0000-000000000000"
	for _, tc := range []struct {
		db            *sql.DB
		id            int64
		token, worker string
	}{
		{nil, 1, token, "worker"}, {db, 0, token, "worker"}, {db, 1, token, " "},
		{db, 1, "bad", "worker"}, {db, 1, "zzzzzzzz-0000-0000-0000-000000000000", "worker"},
		{db, 1, "00000000-0000-0000-0000-0000000000--", "worker"},
	} {
		if _, err := Renew(context.Background(), tc.db, tc.id, tc.token, tc.worker, time.Second); err == nil {
			t.Fatal("invalid renewal accepted")
		}
		if err := Complete(context.Background(), tc.db, tc.id, tc.token, tc.worker, json.RawMessage(`null`)); err == nil {
			t.Fatal("invalid completion accepted")
		}
	}
	for _, lease := range []time.Duration{-time.Second, 0, time.Microsecond} {
		if _, err := Renew(context.Background(), db, 1, token, "worker", lease); err == nil {
			t.Fatal("invalid duration accepted")
		}
	}
	for _, result := range []json.RawMessage{nil, json.RawMessage(`invalid`)} {
		if err := Complete(context.Background(), db, 1, token, "worker", result); err == nil {
			t.Fatal("invalid JSON accepted")
		}
	}
}
