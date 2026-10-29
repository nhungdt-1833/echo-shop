# echo-shop

Mini project of the course "Advance Technical in Go — with Echo Framework".

## Run

```bash
cp .env.example .env          # get JWT_SECRET run command: openssl rand -base64 32
docker compose up -d
go run ./cmd/api              # http://localhost:8080
go run ./cmd/worker payment        # order.created → pay → order.paid
go run ./cmd/worker email          # order.paid → confirmation email
go run ./cmd/worker notify         # order.*    → WebSocket via Redis
go run ./cmd/worker kafka-bridge   # order.*    → Kafka topic order-events
go run ./cmd/worker analytics      # Kafka group "analytics" → stats + ranking

BEHIND_PROXY=true PORT=8080 go run ./cmd/api
BEHIND_PROXY=true PORT=8081 go run ./cmd/api
API_HOST=$(hostname -I | awk '{print $1}') docker compose up -d nginx
JOB_REPORT_SPEC="@every 10s" JOB_CANCEL_SPEC="@every 10s" PENDING_TIMEOUT=1m go run ./cmd/api
go test -race ./...
```
