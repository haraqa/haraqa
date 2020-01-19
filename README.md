Haraqa
===
[![GoDoc](https://godoc.org/github.com/haraqa/haraqa?status.svg)](https://pkg.go.dev/github.com/haraqa/haraqa?tab=doc)
[![Go Version](https://img.shields.io/github/go-mod/go-version/haraqa/haraqa)](https://github.com/haraqa/haraqa/blob/master/go.mod#L3)
[![Go Report Card](https://goreportcard.com/badge/github.com/haraqa/haraqa)](https://goreportcard.com/report/haraqa/haraqa)
[![License](https://img.shields.io/github/license/haraqa/haraqa.svg)](https://github.com/haraqa/haraqa/blob/master/LICENSE)
[![build](https://github.com/haraqa/haraqa/workflows/build/badge.svg)](https://github.com/haraqa/haraqa/blob/master/.github/workflows/go.yml)
[![Docker Build](https://img.shields.io/docker/cloud/build/haraqa/haraqa.svg)](https://hub.docker.com/r/haraqa/haraqa/)
[![Release](https://img.shields.io/github/release/haraqa/haraqa.svg)](https://github.com/haraqa/haraqa/releases)

High Availability Routing And Queueing Application
--------------------------------------------------

![Mascot](https://raw.githubusercontent.com/haraqa/haraqa/media/mascot.png)

**haraqa** is designed to be a developer friendly, scalable message queue for data persistence and communication between microservices.

#### Table of Contents
* [About the Project](#about-the-project)
  * [Overview](#overview)
  * [Persistence and Replication](#persistence-and-replication)
  * [Usecases](#usecases)
* [Getting Started](#getting-started)
  * [Broker](#broker)
  * [Client](#client)
* [Contributing](#contributing)
* [License](#license)

<h2 align="center">About the Project</h2>

### Overview
Haraqa is meant for handling and persisting data in a distributed system. One or more
brokers can be used to send and receive messages. Each broker has a set of 'topics',
a set of messages stored in the order received.

A Haraqa client can produce and/or consume from a broker's topics. These messages
can be produced one at a time or in batches. Messages are consumed by making a request
for a specific offset and limit. The messages can be consumed one at a
time or in batches.

![Diagram](https://raw.githubusercontent.com/haraqa/haraqa/media/diagram.jpg)

### Persistence and Replication
Each broker, after receiving a message from a producer, can save the message to multiple
volumes. These volumes are meant to be distributed in the architecture, such as having
multiple PersistentVolumes in a Kubernetes cluster, EBS in AWS, or Persistent Disks in
Google Cloud. The broker reads messages from the last volume when sending to consumer clients.

When retrieving information about a topic (list topics, find a topic's offsets, watching a topic
for changes, etc) a client makes requests to a gRPC server which returns topic information based
on the last volume.

If a volume is removed or corrupted during a restart the data is repopulated from the other volumes.

![Replication](https://raw.githubusercontent.com/haraqa/haraqa/media/replication.jpg)

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

#### Aggregation for emails or notifications
In this [example](https://github.com/haraqa/haraqa/tree/master/examples/emails) users are emailed
once a day to notify them of the number of profile views they received.

<h2 align="center">Getting started</h2>

### Broker
The recommended deployment strategy is to use [Docker](hub.docker.com/r/haraqa/haraqa)
```
docker run -it -p 4353:4353 -p 14353:14353 -v $PWD/v1:/v1 haraqa/haraqa /v1
```

### Client
```
go get github.com/haraqa/haraqa
```
##### Client Code Examples
Client examples can be found in the
[godoc documentation](https://pkg.go.dev/github.com/haraqa/haraqa?tab=doc#pkg-overview)

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
    ctx    = context.Background()
    topic  = []byte("my_topic")
    msg1   = []byte("hello")
    msg2   = []byte("world")
    offset = 0
    limit  = 2048
  )

  // produce messages in a batch
  err = client.Produce(ctx, topic, msg1, msg2)
  if err != nil {
    panic(err)
  }

  // consume messages in a batch
  msgs, err := client.Consume(ctx, topic, offset, limit, nil)
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