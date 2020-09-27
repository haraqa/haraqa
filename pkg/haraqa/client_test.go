package haraqa

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/haraqa/haraqa/internal/headers"

	"github.com/pkg/errors"
)

func TestOptions(t *testing.T) {
	// WithURL
	{
		// invalid url
		opt := WithURL(string([]byte{0, 1, 2, 255}))
		err := opt(&Client{})
		if err == nil {
			t.Error(err)
		}

		// valid url
		opt = WithURL("http://127.0.0.1:80")
		c := &Client{}
		err = opt(c)
		if err != nil {
			t.Error(err)
		}
		if c.url != "http://127.0.0.1:80" {
			t.Error("http url not set")
		}
	}

	// WithClient
	{
		// invalid client
		opt := WithHTTPClient(nil)
		err := opt(&Client{})
		if err == nil {
			t.Error(err)
		}

		// valid client
		opt = WithHTTPClient(http.DefaultClient)
		c := &Client{}
		err = opt(c)
		if err != nil {
			t.Error(err)
		}
		if c.c != http.DefaultClient {
			t.Error("http client not set")
		}
	}
}

func TestNewClient(t *testing.T) {
	// with default options
	c, err := NewClient()
	if err != nil {
		t.Error(err)
	}
	if c.url != "http://127.0.0.1:4353" {
		t.Error(c.url)
	}
	if c.c == nil {
		t.Error(c.c)
	}

	// with error option
	errTest := errors.New("test errror")
	_, err = NewClient(func(client *Client) error {
		return errTest
	})
	if err != errTest {
		t.Error(err, errTest)
	}
}

func TestClient_InvalidRequests(t *testing.T) {
	c, err := NewClient()
	if err != nil {
		t.Error(err)
	}

	for _, url := range []string{"invalid url", string([]byte{0, 1, 2, 3, 255})} {
		c.url = url
		err = c.CreateTopic("create_topic")
		if err == nil {
			t.Error(err)
		}
		err = c.DeleteTopic("delete_topic")
		if err == nil {
			t.Error(err)
		}
		err = c.ListTopics("", "", "")
		if err == nil {
			t.Error(err)
		}
		err = c.Produce("produce_topic", nil, nil)
		if err == nil {
			t.Error(err)
		}
		_, _, err = c.Consume("consume_topic", 0, 0)
		if err == nil {
			t.Error(err)
		}
	}
}

func TestClient_CreateTopic(t *testing.T) {
	var count int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Error("invalid method")
		}
		if r.URL.String() != "/topics/create_topic" {
			t.Errorf("invalid url path %q", r.URL.String())
		}
		switch count {
		case 0:
			w.WriteHeader(http.StatusCreated)
		case 1:
			headers.SetError(w, headers.ErrTopicAlreadyExists)
		}
		count++
	}))
	ts.EnableHTTP2 = true
	defer ts.Close()

	c, err := NewClient(WithHTTPClient(ts.Client()), WithURL(ts.URL))
	if err != nil {
		t.Error(err)
	}
	err = c.CreateTopic("create_topic")
	if err != nil {
		t.Error(err)
	}
	err = c.CreateTopic("create_topic")
	if !errors.Is(err, headers.ErrTopicAlreadyExists) {
		t.Error(err)
	}
}

func TestClient_DeleteTopic(t *testing.T) {
	var count int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Error("invalid method")
		}
		if r.URL.String() != "/topics/delete_topic" {
			t.Errorf("invalid url path %q", r.URL.String())
		}
		switch count {
		case 0:
			w.WriteHeader(http.StatusNoContent)
		case 1:
			headers.SetError(w, headers.ErrTopicDoesNotExist)
		}
		count++
	}))
	ts.EnableHTTP2 = true
	defer ts.Close()

	c, err := NewClient(WithHTTPClient(ts.Client()), WithURL(ts.URL))
	if err != nil {
		t.Error(err)
	}
	err = c.DeleteTopic("delete_topic")
	if err != nil {
		t.Error(err)
	}
	err = c.DeleteTopic("delete_topic")
	if !errors.Is(err, headers.ErrTopicDoesNotExist) {
		t.Error(err)
	}
}

func TestClient_ListTopics(t *testing.T) {
	var count int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Error("invalid method")
		}
		if r.URL.String() != "/topics?prefix=p&suffix=s&regex=r" {
			t.Errorf("invalid url path %q", r.URL.String())
		}
		switch count {
		case 0:
			w.WriteHeader(http.StatusOK)
		case 1:
			headers.SetError(w, headers.ErrTopicDoesNotExist)
		}
		count++
	}))
	ts.EnableHTTP2 = true
	defer ts.Close()

	c, err := NewClient(WithHTTPClient(ts.Client()), WithURL(ts.URL))
	if err != nil {
		t.Error(err)
	}
	err = c.ListTopics("p", "s", "r")
	if err != nil {
		t.Error(err)
	}
	err = c.ListTopics("p", "s", "r")
	if !errors.Is(err, headers.ErrTopicDoesNotExist) {
		t.Error(err)
	}
}

func TestClient_Produce(t *testing.T) {
	var count int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("invalid method %s", r.Method)
		}
		if r.URL.String() != "/topics/produce_topic" {
			t.Errorf("invalid url path %q", r.URL.String())
		}
		sizes, err := headers.ReadSizes(r.Header)
		if err != nil {
			t.Error(err)
		}
		if len(sizes) != 3 || sizes[0] != 1 || sizes[1] != 3 || sizes[2] != 5 {
			t.Errorf("invalid sizes %+v", sizes)
		}
		switch count {
		case 0, 1:
			b, err := ioutil.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
			}
			if string(b) != "test_body" {
				t.Error(string(b))
			}
			w.WriteHeader(http.StatusOK)
		case 2:
			headers.SetError(w, headers.ErrInvalidHeaderSizes)
		}
		count++
	}))
	ts.EnableHTTP2 = true
	defer ts.Close()

	c, err := NewClient(WithHTTPClient(ts.Client()), WithURL(ts.URL))
	if err != nil {
		t.Error(err)
	}
	err = c.Produce("produce_topic", []int64{1, 3, 5}, bytes.NewBuffer([]byte("test_body")))
	if err != nil {
		t.Error(err)
	}
	err = c.ProduceMsgs("produce_topic", []byte("t"), []byte("est"), []byte("_body"))
	if err != nil {
		t.Error(err)
	}
	err = c.Produce("produce_topic", []int64{1, 3, 5}, nil)
	if !errors.Is(err, headers.ErrInvalidHeaderSizes) {
		t.Error(err)
	}
	err = c.ProduceMsgs("produce_topic")
	if err != nil {
		t.Error(err)
	}
	err = c.ProduceMsgs("produce_topic", nil)
	if err != nil {
		t.Error(err)
	}
}

func TestClient_Consume(t *testing.T) {
	var count int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("invalid method %s", r.Method)
		}
		if r.URL.String() != "/topics/consume_topic?id=123&limit=456" {
			t.Errorf("invalid url path %q", r.URL.String())
		}
		switch count {
		case 0, 3:
			headers.SetSizes([]int64{1, 3, 5}, w.Header())
			w.WriteHeader(http.StatusOK)
			_, err := w.Write([]byte("test_body"))
			if err != nil {
				t.Error(err)
			}
		case 1, 4:
			w.WriteHeader(http.StatusOK)
			_, err := w.Write([]byte("test_body"))
			if err != nil {
				t.Error(err)
			}
		case 2:
			headers.SetError(w, headers.ErrInvalidMessageLimit)
		case 5:
			headers.SetSizes([]int64{1, 3, 5}, w.Header())
			w.WriteHeader(http.StatusOK)
			_, err := w.Write([]byte("test_bod"))
			if err != nil {
				t.Error(err)
			}
		}
		count++
	}))
	ts.EnableHTTP2 = true
	defer ts.Close()

	c, err := NewClient(WithHTTPClient(ts.Client()), WithURL(ts.URL))
	if err != nil {
		t.Error(err)
	}

	// case 0
	body, sizes, err := c.Consume("consume_topic", 123, 456)
	if err != nil {
		t.Error(err)
	}
	if len(sizes) != 3 || sizes[0] != 1 || sizes[1] != 3 || sizes[2] != 5 {
		t.Errorf("invalid sizes %+v", sizes)
	}
	b, err := ioutil.ReadAll(body)
	if err != nil {
		t.Error(err)
	}
	if string(b) != "test_body" {
		t.Error(string(b))
	}

	// case 1
	_, _, err = c.Consume("consume_topic", 123, 456)
	if !errors.Is(err, headers.ErrInvalidHeaderSizes) {
		t.Error(err)
	}

	// case 2
	_, _, err = c.Consume("consume_topic", 123, 456)
	if !errors.Is(err, headers.ErrInvalidMessageLimit) {
		t.Error(err)
	}

	// case 3
	msgs, err := c.ConsumeMsgs("consume_topic", 123, 456)
	if err != nil {
		t.Error(err)
	}
	if len(msgs) != 3 || string(bytes.Join(msgs, nil)) != "test_body" {
		t.Error(msgs)
	}

	// case 4
	_, err = c.ConsumeMsgs("consume_topic", 123, 456)
	if !errors.Is(err, headers.ErrInvalidHeaderSizes) {
		t.Error(err)
	}

	// case 5
	_, err = c.ConsumeMsgs("consume_topic", 123, 456)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Error(err)
	}
}
