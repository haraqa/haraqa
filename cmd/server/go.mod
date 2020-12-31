module main

go 1.14

replace github.com/haraqa/haraqa => ../..

require (
	github.com/haraqa/haraqa v0.0.0-20200927050233-4757a4e5253a
	github.com/prometheus/client_golang v1.9.0
	github.com/sirupsen/logrus v1.7.0
	google.golang.org/protobuf v1.25.0 // indirect
)
