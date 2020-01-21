Examples
===
Extended client examples and use cases
---

# Deployments
## [compose](https://github.com/haraqa/haraqa/tree/master/internal/examples/compose)
Docker compose deployment example for running a local broker instance with prometheus and grafana.

## [k8s](https://github.com/haraqa/haraqa/tree/master/internal/examples/k8s)
Kubernetes deployment example to deploy 2 stateful brokers each with a replication factor of 3.
Replication is split between 2 persistent volumes and the disk local to the broker.

# Extensions
## [encypted](https://github.com/haraqa/haraqa/tree/master/internal/examples/encypted)
The regular `haraqa.Client` is extended to encrypt all messages being produced/consumed
using aes symetric encryption.


# Usecases
## [logs](https://github.com/haraqa/haraqa/tree/master/internal/examples/logs)
The `haraqa.Client` is used to implement a log.Logger extension which produces logs to a topic.

## [emails](https://github.com/haraqa/haraqa/tree/master/internal/examples/emails)
Simple web server records how many views a user's page gets and periodically emails
the user of the number of views

## [message_routing](https://github.com/haraqa/haraqa/tree/master/internal/examples/message_routing)
A web server accepts post requests to add messages to a topic and get requests to read from
the topic. This forms a simple asynchronous chat server.

## [time_series](https://github.com/haraqa/haraqa/tree/master/internal/examples/time_series)
Time series metrics are added in series to a topic to be consumed later


# Client implementations
## [producer_partitions](https://github.com/haraqa/haraqa/tree/master/internal/examples/producer_partitions)
A common topic prefix is extended to multiple topics on the broker. Producers send messages to a single
channel from which multiple ProducerLoops read and send

## [consumer_group](https://github.com/haraqa/haraqa/tree/master/internal/examples/consumer_group)
Multiple consumers use the `Lock` method to load balance messages across multiple consumers.

## [consumer_group_partitions](https://github.com/haraqa/haraqa/tree/master/internal/examples/consumer_group_partitions)
Multiple consumers use the `Lock` method to load balance messages from multiple topics across
multiple consumers.

## [loadtesting](https://github.com/haraqa/haraqa/tree/master/internal/examples/loadtesting)
1 million messages are sent as fast as possible and the time to complete is recorded.
