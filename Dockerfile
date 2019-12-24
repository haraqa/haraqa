# ===================================================
# Compiler image (use debian for -race detection)
# ===================================================
FROM golang:1.13 as compiler
RUN mkdir /profiles && mkdir /haraqa
WORKDIR /haraqa
COPY go.mod .
RUN go mod download
COPY . .
RUN go test ./testing -mod=readonly -race

# Prevent caching when --build-arg CACHEBUST=$$(date +%s)
ARG CACHEBUST=1
ARG TEST_ONLY=""

# test for speed
RUN go test -bench=. -run=XXX -cpu=6 \
  -cpuprofile   /profiles/cpu.out \
  -memprofile   /profiles/mem.out \
  -coverprofile /profiles/cover.out \
  ./testing

# Build binary
RUN if [ "$TEST_ONLY" = "" ] ; \
    then \
      CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags '-s -w' -o /haraqa-build /haraqa/docker/main.go \
    ; fi

# ===================================================
# Output image
# ===================================================
FROM amd64/busybox:1.30.1

# Get binary from compiler
COPY --from=compiler /haraqa-build /haraqa

# Setup Default Behavior
ENTRYPOINT ["/haraqa"]

# Ports - grpc port:4353, streaming port:14353, pprof port: 6060
EXPOSE 4353 14353 6060