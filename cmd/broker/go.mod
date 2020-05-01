module main

go 1.13

replace github.com/haraqa/haraqa => ../..

require (
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/haraqa/haraqa v0.0.0-20200430153726-05d8d9522f38
	github.com/prometheus/client_golang v1.6.0
	golang.org/x/sys v0.0.0-20200430202703-d923437fa56d // indirect
	google.golang.org/genproto v0.0.0-20200430143042-b979b6f78d84 // indirect
	google.golang.org/grpc v1.29.1
)
