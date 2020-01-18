module main

go 1.13

replace github.com/haraqa/haraqa => ../..

require (
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/haraqa/haraqa v0.0.0-20200118182401-6ad45a82bf53
	github.com/pkg/errors v0.9.1 // indirect
	github.com/prometheus/client_golang v1.3.0
	github.com/prometheus/common v0.8.0 // indirect
	golang.org/x/net v0.0.0-20200114155413-6afb5195e5aa // indirect
	golang.org/x/sys v0.0.0-20200117145432-59e60aa80a0c // indirect
	google.golang.org/genproto v0.0.0-20200117163144-32f20d992d24 // indirect
	google.golang.org/grpc v1.26.0
)
