# echo-shop

Mini project of the course "Advance Technical in Go — with Echo Framework".

## Run

```bash
cp .env.example .env          # get JWT_SECRET run command: openssl rand -base64 32
docker compose up -d
go run ./cmd/api              # http://localhost:8080
go test -race ./...
```
