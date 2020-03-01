module main

go 1.13

replace github.com/haraqa/haraqa => ../..

require (
	github.com/golang/mock v1.4.1 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/haraqa/haraqa v0.0.0-20200301204256-e0ebc2c1e007
	github.com/prometheus/client_golang v1.4.1
	github.com/prometheus/procfs v0.0.10 // indirect
	google.golang.org/grpc v1.27.1
)
