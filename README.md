Haraqa
===
[![GoDoc](https://godoc.org/github.com/haraqa/haraqa?status.svg)](https://pkg.go.dev/github.com/haraqa/haraqa?tab=doc)
[![Report Card](https://goreportcard.com/badge/github.com/haraqa/haraqa)](https://goreportcard.com/report/haraqa/haraqa)
[![License](https://img.shields.io/github/license/haraqa/haraqa.svg)](https://github.com/haraqa/haraqa/blob/master/LICENSE)
[![Build](https://github.com/haraqa/haraqa/workflows/build/badge.svg)](https://github.com/haraqa/haraqa/blob/master/.github/workflows/go.yml)
[![Coverage Status](https://coveralls.io/repos/github/haraqa/haraqa/badge.svg)](https://coveralls.io/github/haraqa/haraqa)
[![Docker Build](https://img.shields.io/docker/cloud/build/haraqa/haraqa.svg)](https://hub.docker.com/r/haraqa/haraqa/)
[![Release](https://img.shields.io/github/release/haraqa/haraqa.svg)](https://github.com/haraqa/haraqa/releases)

<h2 align="center">High Availability Routing And Queueing Application</h2>

<div align="center">
  <a href="https://github.com/haraqa/haraqa">
    <img src="https://raw.githubusercontent.com/haraqa/haraqa/media/mascot.png"/>
  </a>
</div>

**haraqa** is designed to be a developer friendly, scalable message queue for data
persistence and real-time data streaming between microservices. Haraqa provides high-throughput,
low-latency, fault-tolerant pipelines for architectures of any size.


### Table of Contents
* [About the Project](#about-the-project)
  * [Overview](#overview)
  * [Persistence and Replication](#persistence-and-replication)
  * [Usecases](#usecases)
* [Getting Started](#getting-started)
  * [API Docs](#api-docs)
  * [Server](#server)
  * [Client](#client)
* [Contributing](#contributing)
* [License](#license)

<h2 align="center">About the Project</h2>

### Overview
Haraqa is meant for handling and persisting data in a distributed system. One or more
servers can be used to send and receive messages. Each server has a set of 'topics',
a set of messages stored in the order received.

A Haraqa client can produce and/or consume from a server's topics. These messages
can be produced one at a time or in batches. Messages are consumed by making a request
for a specific offset and limit. The messages can be consumed one at a
time or in batches.

<div align="center">
  <a href="https://raw.githubusercontent.com/haraqa/haraqa/media/diagram.jpg">
    <img src="https://raw.githubusercontent.com/haraqa/haraqa/media/diagram.jpg"/>
  </a>
</div>

### Persistence and Replication
Each server, after receiving a message from a producer, can save the message to multiple
volumes. These volumes are meant to be distributed in the architecture, such as having
multiple PersistentVolumes in a Kubernetes cluster, EBS in AWS, or Persistent Disks in
Google Cloud. The server reads messages from the last volume when sending to consumer clients.

If a volume is removed or corrupted during a restart the data is repopulated from the other volumes.

<div align="center">
  <a href="https://raw.githubusercontent.com/haraqa/haraqa/media/replication.jpg">
    <img src="https://raw.githubusercontent.com/haraqa/haraqa/media/replication.jpg"/>
  </a>
</div>

### Usecases
* #### Log Aggregation
  * [Example](https://github.com/haraqa/haraqa/tree/master/internal/examples/logs).
  Haraqa can be used by services to persist logs for debugging or auditing.
* #### Message routing between clients
  * [Example](https://github.com/haraqa/haraqa/tree/master/internal/examples/message_routing).
http clients can send and receive messages asynchronously through POST and GET requests
to a simple REST server. These messages are stored in haraqa in a topic unique to each client.
* #### Time series data
  * [Example](https://github.com/haraqa/haraqa/tree/master/internal/examples/time_series).
  Metrics can be stored in a topic and later used for graphing or more complex analysis.
* #### Aggregation for emails or notifications
  * [Example](https://github.com/haraqa/haraqa/tree/master/internal/examples/emails).
  Notifications can be aggregated and sent out in batches for daily/weekly emails or push notifications.

<h2 align="center">Getting started</h2>

### API Docs
* [Redocs API Documentation](https://haraqa.github.io/haraqa/cmd/server/redocs.html)
* [Swagger API Documentation](https://haraqa.github.io/haraqa/cmd/server/swagger.html)
* [Swagger yaml](https://github.com/haraqa/haraqa/blob/master/cmd/server/swagger.yaml)

The docker server also includes local api documentation at the `/docs` and `/docs/swagger` endpoints.

### Server
The recommended deployment strategy is to use [Docker](hub.docker.com/r/haraqa/haraqa)
```
docker run -it -p 4353:4353 -v $PWD/vol1:/vol1 haraqa/haraqa /vol1
```

To run from source, navigate to cmd/server and run the main.go file.
```
cd cmd/server
run main.go vol1
```

<details><summary>Details</summary>
<p>

```
docker run -it [port mapping] [volume mounts] haraqa/haraqa [flags] [volumes]
```

##### Flags:
```
  -http    uint    Port to listen on (default 4353)
  -cache   boolean Enable queue file caching (default true)
  -cors    boolean Enable CORS (default true)
  -docs    boolean Enable Docs pages (default true)
  -entries integer The number of msg entries per queue file before creating a new file (default 5000)
  -limit   integer Default batch limit for consumers (default -1)
  -ballast integer Garbage collection memory ballast size in bytes (default 1073741824)
  -prometheus boolean Enable prometheus metrics (default true)
```

##### Volumes:
Volumes will be written to in the order given and recovered from in the reverse
order. Consumer requests are read from the last volume. For this reason it's
recommended to use a local volume last.

For instance, given
```
docker run haraqa/haraqa /vol1 /vol2 /vol3
```

When a message is received it will be written to /vol1, then /vol2, then /vol3.
When a message is consumed it will be read from /vol3.

During recovery, if data exists in /vol3 it will be replicated to volumes /vol1 and /vol2.
If /vol3 is empty, /vol2 will be replicated to /vol1 and /vol3.

</p>
</details>

##### Documentation
When running the server locally, documentation can be found at the /docs endpoint
* [Redocs documentation](localhost:4353/docs)
* [Redocs documentation](localhost:4353/docs/swagger)

### Client
```
go get github.com/haraqa/haraqa
```
##### Client Code Examples
Client examples can be found in the
[godoc documentation](https://pkg.go.dev/github.com/haraqa/haraqa?tab=doc#pkg-overview)

##### Additional Examples
Additional examples are located in the internal examples folder [internal/examples](https://github.com/haraqa/haraqa/tree/master/internal/examples)

<details><summary>Hello World Quickstart</summary>
<p>

```
package main

import (
  "context"
  "log"

  "github.com/haraqa/haraqa"
)

func main() {
  client, err := haraqa.NewClient(haraqa.WithAddr("127.0.0.1"))
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

</p>
</details>

#### Command Line Client

See the [hrqa repository](https://github.com/haraqa/hrqa) for more details

```
go get github.com/haraqa/hrqa
```

<h2 align="center">Contributing</h2>

We want this project to be the best it can be and all feedback, feature requests or pull requests are welcome.

<h2 align="center">License</h2>

MIT © 2019 [haraqa](https://github.com/haraqa/) and [contributors](https://github.com/haraqa/haraqa/graphs/contributors). See `LICENSE` for more information.