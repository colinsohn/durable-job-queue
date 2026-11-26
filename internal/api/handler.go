package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"

	"durable-job-queue/internal/queue"
)

type JobStore interface {
	Submit(context.Context, string, queue.Submission) (queue.Job, bool, error)
	Get(context.Context, int64) (queue.Job, error)
	Diagnostics(context.Context) (queue.Diagnostics, error)
}

func Handler(store JobStore) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("Idempotency-Key")
		if strings.TrimSpace(key) == "" || len(key) > 200 {
			fail(w, 400, "Idempotency-Key must contain 1–200 bytes")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		var req queue.Submission
		if err := dec.Decode(&req); err != nil {
			fail(w, 400, "invalid JSON request (maximum 1 MiB)")
			return
		}
		if err := dec.Decode(new(any)); err != io.EOF {
			fail(w, 400, "expected one JSON value")
			return
		}
		if req.Type != "echo" || !json.Valid(req.Payload) {
			fail(w, 400, "type must be echo and payload must be valid JSON")
			return
		}
		j, created, err := store.Submit(r.Context(), key, req)
		if errors.Is(err, queue.ErrConflict) {
			fail(w, 409, err.Error())
			return
		}
		if err != nil {
			log.Printf("submit: %v", err)
			fail(w, 500, "database request failed")
			return
		}
		status := http.StatusOK
		if created {
			status = http.StatusCreated
		}
		w.Header().Set("Location", "/jobs/"+strconv.FormatInt(j.ID, 10))
		respond(w, status, j)
	})
	mux.HandleFunc("GET /jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if err != nil || id < 1 {
			fail(w, 400, "job ID must be a positive integer")
			return
		}
		j, err := store.Get(r.Context(), id)
		if errors.Is(err, queue.ErrNotFound) {
			fail(w, 404, err.Error())
			return
		}
		if err != nil {
			log.Printf("get job: %v", err)
			fail(w, 500, "database request failed")
			return
		}
		respond(w, 200, j)
	})
	mux.HandleFunc("GET /diagnostics", func(w http.ResponseWriter, r *http.Request) {
		d, err := store.Diagnostics(r.Context())
		if err != nil {
			log.Printf("diagnostics: %v", err)
			fail(w, 500, "database request failed")
			return
		}
		respond(w, 200, d)
	})
	return mux
}

func respond(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func fail(w http.ResponseWriter, status int, message string) {
	respond(w, status, map[string]string{"error": message})
}
