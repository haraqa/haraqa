# Logs

This example creates new loggers used to log errors and warnings to a haraqa cluster.

```bash
go run examples/logs/main.go
curl 'http://localhost:4353/topics/example_errors' -H 'X-Id: -1'
curl 'http://localhost:4353/topics/example_warn'   -H 'X-Id: -1'
```