# syntax=docker/dockerfile:1

FROM golang:1.23-bookworm AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal
COPY db ./db

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" -o /out/queue-api ./cmd/queue-api && \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" -o /out/queue-worker ./cmd/queue-worker

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app

COPY --from=build /out/queue-api /app/queue-api
COPY --from=build /out/queue-worker /app/queue-worker
COPY --from=build /src/db /app/db

USER nonroot:nonroot
ENV PORT=8080
EXPOSE 8080

ENTRYPOINT ["/app/queue-api"]
