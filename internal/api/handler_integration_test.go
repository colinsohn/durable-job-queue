package api

import (
	"context"
	"database/sql"
	"durable-job-queue/internal/queue"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSubmissionPostgres(t *testing.T) {
	dsn := os.Getenv("QUEUE_TEST_DSN")
	if dsn == "" {
		t.Skip("set QUEUE_TEST_DSN for PostgreSQL integration tests")
	}
	admin, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("queue_api_%d", time.Now().UnixNano())
	if _, err = admin.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(`DROP SCHEMA ` + schema + ` CASCADE`)
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	db, err := sql.Open("postgres", dsn+sep+"search_path="+schema)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(24)
	ddl, err := os.ReadFile("../../db/schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(string(ddl)); err != nil {
		t.Fatal(err)
	}
	h := Handler(queue.Store{DB: db})
	request := func(method, path, key, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Idempotency-Key", key)
		res := httptest.NewRecorder()
		h.ServeHTTP(res, req)
		return res
	}
	const body = `{"type":"echo","payload":{"a":1,"b":2}}`
	var wg sync.WaitGroup
	results := make(chan *httptest.ResponseRecorder, 24)
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); results <- request("POST", "/jobs", "same", body) }()
	}
	wg.Wait()
	close(results)
	created, id := 0, int64(0)
	for res := range results {
		if res.Code == 201 {
			created++
		} else if res.Code != 200 {
			t.Fatalf("concurrent submission: %d %s", res.Code, res.Body)
		}
		var j queue.Job
		if err = json.Unmarshal(res.Body.Bytes(), &j); err != nil {
			t.Fatal(err)
		}
		if id == 0 {
			id = j.ID
		}
		if j.ID != id {
			t.Fatal("duplicate jobs")
		}
		if res.Header().Get("Location") != fmt.Sprintf("/jobs/%d", id) {
			t.Fatal("missing location")
		}
	}
	if created != 1 {
		t.Fatalf("created responses: %d", created)
	}
	for _, tc := range []struct {
		method, path, key, body string
		code                    int
	}{
		{"POST", "/jobs", "same", `{"payload":{"b":2.0,"a":1},"type":"echo"}`, 200},
		{"POST", "/jobs", "same", `{"payload":{"a":2},"type":"echo"}`, 409},
		{"POST", "/jobs", "", body, 400},
		{"POST", "/jobs", "new", `{"type":"bad","payload":{}}`, 400},
		{"POST", "/jobs", "new", `{"type":"echo"}`, 400},
		{"POST", "/jobs", "new", body + ` {}`, 400},
		{"POST", "/jobs", "new", `{"type":"echo","payload":{},"extra":1}`, 400},
		{"POST", "/jobs", "new", `{"type":"echo","payload":"` + strings.Repeat("x", 1<<20) + `"}`, 400},
		{"GET", fmt.Sprintf("/jobs/%d", id), "", "", 200},
		{"GET", "/jobs/999999", "", "", 404},
		{"GET", "/jobs/0", "", "", 400},
		{"GET", "/jobs/nope", "", "", 400},
	} {
		res := request(tc.method, tc.path, tc.key, tc.body)
		if res.Code != tc.code {
			t.Errorf("%s %s: got %d want %d: %s", tc.method, tc.path, res.Code, tc.code, res.Body)
		}
	}
	claim, err := queue.ClaimNext(context.Background(), db, "worker", time.Minute)
	if err != nil || claim == nil {
		t.Fatalf("claim %v: %v", claim, err)
	}
	res := request("POST", "/jobs", "same", body)
	var j queue.Job
	if err = json.Unmarshal(res.Body.Bytes(), &j); err != nil {
		t.Fatal(err)
	}
	if res.Code != 200 || j.State != "leased" || j.AttemptsStarted != 1 {
		t.Fatalf("replay reset job: %s", res.Body)
	}
	var count int
	if err = db.QueryRow(`SELECT count(*) FROM queue_jobs`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("count %d: %v", count, err)
	}
	if res = request("POST", "/jobs", "distinct", body); res.Code != 201 {
		t.Fatalf("distinct key: %d", res.Code)
	}
	db.Close()
	if res = request("GET", "/jobs/1", "", ""); res.Code != 500 || strings.Contains(res.Body.String(), "sql:") {
		t.Fatalf("DB failure response: %s", res.Body)
	}
}
