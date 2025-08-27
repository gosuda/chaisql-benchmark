BINARY=sqlbench
BUILD_TAGS=bench

.PHONY: build run clean pg-up pg-down

build:
go build -tags $(BUILD_TAGS) -o $(BINARY) ./cmd/sqlbench

run:
./$(BINARY) -h

clean:
rm -f $(BINARY)

pg-up:
docker compose up -d

pg-down:
docker compose down -v