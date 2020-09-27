module main

go 1.14

replace github.com/haraqa/haraqa => ../..

require (
	github.com/haraqa/haraqa v0.0.0-20200927050233-4757a4e5253a
	github.com/prometheus/client_golang v1.7.1
	github.com/prometheus/common v0.14.0 // indirect
	github.com/prometheus/procfs v0.2.0 // indirect
	google.golang.org/protobuf v1.25.0 // indirect
)
