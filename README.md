Haraqa
======
High Availability Routing And Queueing Application
--------------------------------------------------

![Mascot](mascot.png)

[![GoDoc](https://godoc.org/github.com/haraqa/haraqa?status.svg)](https://godoc.org/github.com/haraqa/haraqa)
[![Go Report Card](https://goreportcard.com/badge/github.com/haraqa/haraqa)](https://goreportcard.com/report/haraqa/protocol)
[![Release](https://img.shields.io/github/release/haraqa/haraqa.svg)](https://github.com/haraqa/haraqa/releases)
[![License](https://img.shields.io/github/license/haraqa/haraqa.svg)](https://github.com/haraqa/haraqa/blob/master/LICENSE)
[![stability-experimental](https://img.shields.io/badge/stability-experimental-orange.svg)](https://github.com/emersion/stability-badges#experimental)
[![Build Status](https://travis-ci.org/haraqa/haraqa.svg?branch=master)](https://travis-ci.org/haraqa/haraqa)
[![Docker Build](https://img.shields.io/docker/build/haraqa/haraqa.svg)](https://hub.docker.com/r/haraqa/haraqa/)
[![Build Status](https://images.microbadger.com/badges/image/haraqa/haraqa.svg)](https://microbadger.com/images/haraqa/haraqa)
[![Releases](https://img.shields.io/github/release/haraqa/haraqa.svg)](https://github.com/haraqa/haraqa/releases)

**haraqa** is designed to be a developer friendly, highly scalable message queue for data persistence and communication between microservices.

#### Table of Contents
* [About the Project](#about-the-project)
  * [Built With](#built-with)
* [Getting Started](#getting-started)
  * [Client](#client)
  * [Broker](#broker)
  * [Prerequisites](#prerequisites)
  * [Installation](#installation)
* [Usage](#usage)
* [Contributing](#contributing)
* [License](#license)

## Getting started
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
  config := haraqa.Config{

  }
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
  )

  // produce messages in a batch
  err = client.Produce(ctx, topic, msg1, msg2)
  if err != nil {
    panic(err)
  }

  // consume messages in a batch
  msgs, err := client.Consume(ctx, topic)
  if err != nil {
    panic(err)
  }

  log.Println(msgs)
}
```
### Broker
The recommended deployment strategy is to use [Docker](hub.docker.com/r/haraqa/haraqa)
#### Locally
```
docker run -it -p 4353:4353 -p 14353:14353 -v $PWD/v1:/v1 haraqa/haraqa /v1
```
#### Kubernetes
```

```

## Contributing
We want this project to be the best it can be and all feedback, feature requests or pull requests are welcome.

## License
MIT © 2019 [haraqa](https://github.com/haraqa/) and [contributors](https://github.com/haraqa/haraqa/graphs/contributors). See `LICENSE` for more information.