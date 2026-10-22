# Stage 1: Builder
FROM golang:alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /app

COPY go.mod go.sum* ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/tg_proxy ./cmd/server

# Stage 2: Lightweight runtime
FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata
WORKDIR /app

COPY --from=builder /app/tg_proxy /app/tg_proxy

EXPOSE 8080

VOLUME ["/tmp/tg_file_proxy"]

ENTRYPOINT ["/app/tg_proxy"]
