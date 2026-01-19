BIN := dbsqlx
TEST_ARGS :=
ifeq ($(VERBOSE),1)
TEST_ARGS += -v
endif

.PHONY: test build clean

test:
	go test $(TEST_ARGS) ./...

build:
	go build -o $(BIN) main.go

clean:
	go clean -testcache
