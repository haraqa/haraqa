package server

//go:generate go run github.com/golang/mock/mockgen -source consumer_manager.go -package server -destination consumer_manager_mock_test.go
//go:generate go run golang.org/x/tools/cmd/goimports -w consumer_manager_mock_test.go

type ConsumerManager interface {
	SetOffset(group, topic string, id int64) error
	GetOffset(group, topic string, reqID int64) (id int64, Unlock func(), err error)
}
