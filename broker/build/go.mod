module main

go 1.13

replace github.com/haraqa/haraqa => ../..

require (
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/haraqa/haraqa v0.0.0-20200426235833-b701e2359f29
	github.com/prometheus/client_golang v1.5.1
	github.com/prometheus/procfs v0.0.11 // indirect
	google.golang.org/grpc v1.29.1
)
