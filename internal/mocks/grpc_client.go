// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/haraqa/haraqa/internal/protocol (interfaces: HaraqaClient,Haraqa_WatchTopicsClient,Haraqa_LockClient)

// Package mocks is a generated GoMock package.
package mocks

import (
	context "context"
	gomock "github.com/golang/mock/gomock"
	protocol "github.com/haraqa/haraqa/internal/protocol"
	grpc "google.golang.org/grpc"
	metadata "google.golang.org/grpc/metadata"
	reflect "reflect"
)

// MockHaraqaClient is a mock of HaraqaClient interface
type MockHaraqaClient struct {
	ctrl     *gomock.Controller
	recorder *MockHaraqaClientMockRecorder
}

// MockHaraqaClientMockRecorder is the mock recorder for MockHaraqaClient
type MockHaraqaClientMockRecorder struct {
	mock *MockHaraqaClient
}

// NewMockHaraqaClient creates a new mock instance
func NewMockHaraqaClient(ctrl *gomock.Controller) *MockHaraqaClient {
	mock := &MockHaraqaClient{ctrl: ctrl}
	mock.recorder = &MockHaraqaClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockHaraqaClient) EXPECT() *MockHaraqaClientMockRecorder {
	return m.recorder
}

// CreateTopic mocks base method
func (m *MockHaraqaClient) CreateTopic(arg0 context.Context, arg1 *protocol.CreateTopicRequest, arg2 ...grpc.CallOption) (*protocol.CreateTopicResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "CreateTopic", varargs...)
	ret0, _ := ret[0].(*protocol.CreateTopicResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CreateTopic indicates an expected call of CreateTopic
func (mr *MockHaraqaClientMockRecorder) CreateTopic(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateTopic", reflect.TypeOf((*MockHaraqaClient)(nil).CreateTopic), varargs...)
}

// DeleteTopic mocks base method
func (m *MockHaraqaClient) DeleteTopic(arg0 context.Context, arg1 *protocol.DeleteTopicRequest, arg2 ...grpc.CallOption) (*protocol.DeleteTopicResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "DeleteTopic", varargs...)
	ret0, _ := ret[0].(*protocol.DeleteTopicResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DeleteTopic indicates an expected call of DeleteTopic
func (mr *MockHaraqaClientMockRecorder) DeleteTopic(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteTopic", reflect.TypeOf((*MockHaraqaClient)(nil).DeleteTopic), varargs...)
}

// ListTopics mocks base method
func (m *MockHaraqaClient) ListTopics(arg0 context.Context, arg1 *protocol.ListTopicsRequest, arg2 ...grpc.CallOption) (*protocol.ListTopicsResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "ListTopics", varargs...)
	ret0, _ := ret[0].(*protocol.ListTopicsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListTopics indicates an expected call of ListTopics
func (mr *MockHaraqaClientMockRecorder) ListTopics(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListTopics", reflect.TypeOf((*MockHaraqaClient)(nil).ListTopics), varargs...)
}

// Lock mocks base method
func (m *MockHaraqaClient) Lock(arg0 context.Context, arg1 ...grpc.CallOption) (protocol.Haraqa_LockClient, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0}
	for _, a := range arg1 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Lock", varargs...)
	ret0, _ := ret[0].(protocol.Haraqa_LockClient)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Lock indicates an expected call of Lock
func (mr *MockHaraqaClientMockRecorder) Lock(arg0 interface{}, arg1 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0}, arg1...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Lock", reflect.TypeOf((*MockHaraqaClient)(nil).Lock), varargs...)
}

// Offsets mocks base method
func (m *MockHaraqaClient) Offsets(arg0 context.Context, arg1 *protocol.OffsetRequest, arg2 ...grpc.CallOption) (*protocol.OffsetResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Offsets", varargs...)
	ret0, _ := ret[0].(*protocol.OffsetResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Offsets indicates an expected call of Offsets
func (mr *MockHaraqaClientMockRecorder) Offsets(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Offsets", reflect.TypeOf((*MockHaraqaClient)(nil).Offsets), varargs...)
}

// TruncateTopic mocks base method
func (m *MockHaraqaClient) TruncateTopic(arg0 context.Context, arg1 *protocol.TruncateTopicRequest, arg2 ...grpc.CallOption) (*protocol.TruncateTopicResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "TruncateTopic", varargs...)
	ret0, _ := ret[0].(*protocol.TruncateTopicResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// TruncateTopic indicates an expected call of TruncateTopic
func (mr *MockHaraqaClientMockRecorder) TruncateTopic(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TruncateTopic", reflect.TypeOf((*MockHaraqaClient)(nil).TruncateTopic), varargs...)
}

// WatchTopics mocks base method
func (m *MockHaraqaClient) WatchTopics(arg0 context.Context, arg1 ...grpc.CallOption) (protocol.Haraqa_WatchTopicsClient, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0}
	for _, a := range arg1 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "WatchTopics", varargs...)
	ret0, _ := ret[0].(protocol.Haraqa_WatchTopicsClient)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// WatchTopics indicates an expected call of WatchTopics
func (mr *MockHaraqaClientMockRecorder) WatchTopics(arg0 interface{}, arg1 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0}, arg1...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WatchTopics", reflect.TypeOf((*MockHaraqaClient)(nil).WatchTopics), varargs...)
}

// MockHaraqa_WatchTopicsClient is a mock of Haraqa_WatchTopicsClient interface
type MockHaraqa_WatchTopicsClient struct {
	ctrl     *gomock.Controller
	recorder *MockHaraqa_WatchTopicsClientMockRecorder
}

// MockHaraqa_WatchTopicsClientMockRecorder is the mock recorder for MockHaraqa_WatchTopicsClient
type MockHaraqa_WatchTopicsClientMockRecorder struct {
	mock *MockHaraqa_WatchTopicsClient
}

// NewMockHaraqa_WatchTopicsClient creates a new mock instance
func NewMockHaraqa_WatchTopicsClient(ctrl *gomock.Controller) *MockHaraqa_WatchTopicsClient {
	mock := &MockHaraqa_WatchTopicsClient{ctrl: ctrl}
	mock.recorder = &MockHaraqa_WatchTopicsClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockHaraqa_WatchTopicsClient) EXPECT() *MockHaraqa_WatchTopicsClientMockRecorder {
	return m.recorder
}

// CloseSend mocks base method
func (m *MockHaraqa_WatchTopicsClient) CloseSend() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CloseSend")
	ret0, _ := ret[0].(error)
	return ret0
}

// CloseSend indicates an expected call of CloseSend
func (mr *MockHaraqa_WatchTopicsClientMockRecorder) CloseSend() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CloseSend", reflect.TypeOf((*MockHaraqa_WatchTopicsClient)(nil).CloseSend))
}

// Context mocks base method
func (m *MockHaraqa_WatchTopicsClient) Context() context.Context {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Context")
	ret0, _ := ret[0].(context.Context)
	return ret0
}

// Context indicates an expected call of Context
func (mr *MockHaraqa_WatchTopicsClientMockRecorder) Context() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Context", reflect.TypeOf((*MockHaraqa_WatchTopicsClient)(nil).Context))
}

// Header mocks base method
func (m *MockHaraqa_WatchTopicsClient) Header() (metadata.MD, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Header")
	ret0, _ := ret[0].(metadata.MD)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Header indicates an expected call of Header
func (mr *MockHaraqa_WatchTopicsClientMockRecorder) Header() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Header", reflect.TypeOf((*MockHaraqa_WatchTopicsClient)(nil).Header))
}

// Recv mocks base method
func (m *MockHaraqa_WatchTopicsClient) Recv() (*protocol.WatchResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Recv")
	ret0, _ := ret[0].(*protocol.WatchResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Recv indicates an expected call of Recv
func (mr *MockHaraqa_WatchTopicsClientMockRecorder) Recv() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Recv", reflect.TypeOf((*MockHaraqa_WatchTopicsClient)(nil).Recv))
}

// RecvMsg mocks base method
func (m *MockHaraqa_WatchTopicsClient) RecvMsg(arg0 interface{}) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RecvMsg", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// RecvMsg indicates an expected call of RecvMsg
func (mr *MockHaraqa_WatchTopicsClientMockRecorder) RecvMsg(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RecvMsg", reflect.TypeOf((*MockHaraqa_WatchTopicsClient)(nil).RecvMsg), arg0)
}

// Send mocks base method
func (m *MockHaraqa_WatchTopicsClient) Send(arg0 *protocol.WatchRequest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Send", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Send indicates an expected call of Send
func (mr *MockHaraqa_WatchTopicsClientMockRecorder) Send(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Send", reflect.TypeOf((*MockHaraqa_WatchTopicsClient)(nil).Send), arg0)
}

// SendMsg mocks base method
func (m *MockHaraqa_WatchTopicsClient) SendMsg(arg0 interface{}) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SendMsg", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// SendMsg indicates an expected call of SendMsg
func (mr *MockHaraqa_WatchTopicsClientMockRecorder) SendMsg(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SendMsg", reflect.TypeOf((*MockHaraqa_WatchTopicsClient)(nil).SendMsg), arg0)
}

// Trailer mocks base method
func (m *MockHaraqa_WatchTopicsClient) Trailer() metadata.MD {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Trailer")
	ret0, _ := ret[0].(metadata.MD)
	return ret0
}

// Trailer indicates an expected call of Trailer
func (mr *MockHaraqa_WatchTopicsClientMockRecorder) Trailer() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Trailer", reflect.TypeOf((*MockHaraqa_WatchTopicsClient)(nil).Trailer))
}

// MockHaraqa_LockClient is a mock of Haraqa_LockClient interface
type MockHaraqa_LockClient struct {
	ctrl     *gomock.Controller
	recorder *MockHaraqa_LockClientMockRecorder
}

// MockHaraqa_LockClientMockRecorder is the mock recorder for MockHaraqa_LockClient
type MockHaraqa_LockClientMockRecorder struct {
	mock *MockHaraqa_LockClient
}

// NewMockHaraqa_LockClient creates a new mock instance
func NewMockHaraqa_LockClient(ctrl *gomock.Controller) *MockHaraqa_LockClient {
	mock := &MockHaraqa_LockClient{ctrl: ctrl}
	mock.recorder = &MockHaraqa_LockClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockHaraqa_LockClient) EXPECT() *MockHaraqa_LockClientMockRecorder {
	return m.recorder
}

// CloseSend mocks base method
func (m *MockHaraqa_LockClient) CloseSend() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CloseSend")
	ret0, _ := ret[0].(error)
	return ret0
}

// CloseSend indicates an expected call of CloseSend
func (mr *MockHaraqa_LockClientMockRecorder) CloseSend() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CloseSend", reflect.TypeOf((*MockHaraqa_LockClient)(nil).CloseSend))
}

// Context mocks base method
func (m *MockHaraqa_LockClient) Context() context.Context {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Context")
	ret0, _ := ret[0].(context.Context)
	return ret0
}

// Context indicates an expected call of Context
func (mr *MockHaraqa_LockClientMockRecorder) Context() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Context", reflect.TypeOf((*MockHaraqa_LockClient)(nil).Context))
}

// Header mocks base method
func (m *MockHaraqa_LockClient) Header() (metadata.MD, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Header")
	ret0, _ := ret[0].(metadata.MD)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Header indicates an expected call of Header
func (mr *MockHaraqa_LockClientMockRecorder) Header() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Header", reflect.TypeOf((*MockHaraqa_LockClient)(nil).Header))
}

// Recv mocks base method
func (m *MockHaraqa_LockClient) Recv() (*protocol.LockResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Recv")
	ret0, _ := ret[0].(*protocol.LockResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Recv indicates an expected call of Recv
func (mr *MockHaraqa_LockClientMockRecorder) Recv() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Recv", reflect.TypeOf((*MockHaraqa_LockClient)(nil).Recv))
}

// RecvMsg mocks base method
func (m *MockHaraqa_LockClient) RecvMsg(arg0 interface{}) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RecvMsg", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// RecvMsg indicates an expected call of RecvMsg
func (mr *MockHaraqa_LockClientMockRecorder) RecvMsg(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RecvMsg", reflect.TypeOf((*MockHaraqa_LockClient)(nil).RecvMsg), arg0)
}

// Send mocks base method
func (m *MockHaraqa_LockClient) Send(arg0 *protocol.LockRequest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Send", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Send indicates an expected call of Send
func (mr *MockHaraqa_LockClientMockRecorder) Send(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Send", reflect.TypeOf((*MockHaraqa_LockClient)(nil).Send), arg0)
}

// SendMsg mocks base method
func (m *MockHaraqa_LockClient) SendMsg(arg0 interface{}) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SendMsg", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// SendMsg indicates an expected call of SendMsg
func (mr *MockHaraqa_LockClientMockRecorder) SendMsg(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SendMsg", reflect.TypeOf((*MockHaraqa_LockClient)(nil).SendMsg), arg0)
}

// Trailer mocks base method
func (m *MockHaraqa_LockClient) Trailer() metadata.MD {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Trailer")
	ret0, _ := ret[0].(metadata.MD)
	return ret0
}

// Trailer indicates an expected call of Trailer
func (mr *MockHaraqa_LockClientMockRecorder) Trailer() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Trailer", reflect.TypeOf((*MockHaraqa_LockClient)(nil).Trailer))
}
