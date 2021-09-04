# ===================================================
# Compiler image (use debian for -race detection)
# ===================================================
FROM golang:1.17 as compiler
RUN mkdir /profiles && mkdir -p /haraqa/cmd/server
WORKDIR /haraqa
COPY go.mod .
COPY cmd/server/go.mod cmd/server/go.mod
RUN go mod download && cd /haraqa/cmd/server && go mod download
COPY . .

# Prevent caching when --build-arg CACHEBUST=$$(date +%s)
ARG CACHEBUST=1
ARG TEST_ONLY=""

# test for behavior
RUN go test -mod=readonly -race -timeout 30s -count=1 -failfast -coverprofile=cover.out -covermode=atomic -tags skip_docker ./... && \
      go tool cover -html=cover.out -o /profiles/coverage.html
# test for speed
RUN go test -mod=readonly -timeout 30s -bench=Produce -benchtime=1000x -run=XXX -cpu=4 \
      -trace        /profiles/produce_trace.out \
      -cpuprofile   /profiles/produce_cpu.out \
      -memprofile   /profiles/produce_mem.out \
      ./internal/benchmarks
RUN go test -mod=readonly -timeout 30s -bench=Consume -benchtime=1000x -run=XXX -cpu=4 \
      -trace        /profiles/consume_trace.out \
      -cpuprofile   /profiles/consume_cpu.out \
      -memprofile   /profiles/consume_mem.out \
      ./internal/benchmarks

# Build binary
RUN if [ -z "$TEST_ONLY" ] ; then \
      cd cmd/server && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -mod=readonly -ldflags '-s -w' -o /haraqa-build main.go \
    ; fi

# ===================================================
# Output image
# ===================================================
FROM busybox:1.31.1

# Setup Default Behavior
ENTRYPOINT ["/haraqa"]

# HTTP Port
EXPOSE 4353

# Get binary from compiler
COPY --from=compiler /haraqa-build /haraqa
