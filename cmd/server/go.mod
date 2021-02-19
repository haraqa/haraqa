module main

go 1.16

replace github.com/haraqa/haraqa => ../..

require (
	github.com/haraqa/haraqa v0.0.0-20210217084740-facb62c24f5c
	github.com/magefile/mage v1.11.0 // indirect
	github.com/prometheus/client_golang v1.9.0
	github.com/prometheus/procfs v0.6.0 // indirect
	github.com/sirupsen/logrus v1.7.1
	google.golang.org/protobuf v1.25.0 // indirect
)
