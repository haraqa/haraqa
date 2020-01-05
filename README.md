Haraqa
======
High Availability Routing And Queueing Application
--------------------------------------------------

![Mascot](https://raw.githubusercontent.com/haraqa/haraqa/master/mascot.png)

[![GoDoc](https://godoc.org/github.com/haraqa/haraqa?status.svg)](https://godoc.org/github.com/haraqa/haraqa)
[![Go Version](https://img.shields.io/github/go-mod/go-version/haraqa/haraqa)](https://github.com/haraqa/haraqa/blob/master/go.mod#L3)
[![Go Report Card](https://goreportcard.com/badge/github.com/haraqa/haraqa)](https://goreportcard.com/report/haraqa/haraqa)
[![Release](https://img.shields.io/github/release/haraqa/haraqa.svg)](https://github.com/haraqa/haraqa/releases)
[![License](https://img.shields.io/github/license/haraqa/haraqa.svg)](https://github.com/haraqa/haraqa/blob/master/LICENSE)
[![stability-unstable](https://img.shields.io/badge/stability-unstable-yellow.svg)](https://github.com/emersion/stability-badges#unstable)
[![Docker Build](https://img.shields.io/docker/cloud/build/haraqa/haraqa.svg)](https://hub.docker.com/r/haraqa/haraqa/)

**haraqa** is designed to be a developer friendly, highly scalable message queue for data persistence and communication between microservices.

#### Table of Contents
* [About the Project](#about-the-project)
  * [Usecases](#usecases)
* [Getting Started](#getting-started)
  * [Broker](#broker)
  * [Client](#client)
* [Contributing](#contributing)
* [License](#license)

## About the Project
### Usecases
#### Log Aggregation
Haraqa can be used by services to persist logs for debugging or auditing. See the
[example](https://github.com/haraqa/haraqa/tree/master/examples/logs) for more information.

#### Message routing between clients
In this [example](https://github.com/haraqa/haraqa/tree/master/examples/message_routing)
http clients can send and receive messages asynchronously through POST and GET requests
to a simple REST server. These messages are stored in haraqa in a topic unique to each client.

#### Time series data
Metrics can be stored in a topic and later used for graphing or more complex analysis.
This [example](https://github.com/haraqa/haraqa/tree/master/examples/time_series) stores
runtime.MemStats data every second.

## Getting started
### Broker
The recommended deployment strategy is to use [Docker](hub.docker.com/r/haraqa/haraqa)
```
docker run -it -p 4353:4353 -p 14353:14353 -v $PWD/v1:/v1 haraqa/haraqa /v1
```

### Client
```
go get github.com/haraqa/haraqa
```
##### Hello World
```
package main

import (
  "context"
  "log"

  "github.com/haraqa/haraqa"
)

func main() {
  config := haraqa.DefaultConfig
  client, err := haraqa.NewClient(config)
  if err != nil {
    panic(err)
  }
  defer client.Close()

  var (
    ctx = context.Background()
    topic = []byte("my_topic")
    msg1 = []byte("hello")
    msg2 = []byte("world")
    offset = 0
  )

  // produce messages in a batch
  err = client.Produce(ctx, topic, msg1, msg2)
  if err != nil {
    panic(err)
  }

  // consume messages in a batch
  resp := haraqa.ConsumeResponse{}
  err = client.Consume(ctx, topic, offset, maxBatchSize, &resp)
  if err != nil {
    panic(err)
  }

  msgs, err := resp.Batch()
  if err != nil {
    panic(err)
  }

  log.Println(msgs)
}
```

## Contributing
We want this project to be the best it can be and all feedback, feature requests or pull requests are welcome.

## License
MIT © 2019 [haraqa](https://github.com/haraqa/) and [contributors](https://github.com/haraqa/haraqa/graphs/contributors). See `LICENSE` for more information.