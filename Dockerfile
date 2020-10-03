# ===================================================
# Compiler image (use debian for -race detection)
# ===================================================
FROM golang:1.15 as compiler
RUN mkdir /profiles && mkdir /haraqa
WORKDIR /haraqa
COPY go.mod .
RUN go mod download
#&& cd cmd/broker && go mod download
COPY . .

# Prevent caching when --build-arg CACHEBUST=$$(date +%s)
ARG CACHEBUST=1
ARG TEST_ONLY=""

# test for behavior
RUN go test -mod=readonly -race -timeout 30s -count=1 -failfast -coverprofile=cover.out -covermode=atomic ./... && \
      go tool cover -html=cover.out -o /profiles/coverage.html
# test for speed
RUN go test -mod=readonly -bench=. -benchtime=1000x -run=XXX -cpu=4 \
      -trace        /profiles/trace.out \
      -cpuprofile   /profiles/cpu.out \
      -memprofile   /profiles/mem.out \
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

# Copy static files
COPY cmd/server/swagger.yaml .
COPY cmd/server/swagger.html .
COPY cmd/server/redocs.html .

# Update to use local version, not github version
RUN sed -i 's/https:\/\/raw.githubusercontent.com\/haraqa\/haraqa\/master\/cmd\/server/\/docs/g' swagger.html && \
    sed -i 's/https:\/\/raw.githubusercontent.com\/haraqa\/haraqa\/master\/cmd\/server/\/docs/g' redocs.html

# Get binary from compiler
COPY --from=compiler /haraqa-build /haraqa
