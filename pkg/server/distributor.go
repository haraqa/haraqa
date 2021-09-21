package server

//go:generate go run github.com/golang/mock/mockgen -source distributor.go -package server -destination distributor_mock_test.go
//go:generate go run golang.org/x/tools/cmd/goimports -w distributor_mock_test.go

type Distributor interface {
	GetTopicOwner(topic string) (string, error)
}

type noopDistributor struct{}

func (d noopDistributor) GetTopicOwner(topic string) (string, error) { return "", nil }
