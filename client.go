package haraqa

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	urlpkg "net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pkg/errors"

	"github.com/haraqa/haraqa/internal/headers"
)

var (
	ErrTopicAlreadyExists = headers.ErrTopicAlreadyExists
	ErrNoContent          = headers.ErrNoContent
	ErrInvalidTopic       = headers.ErrInvalidTopic
)

// Option represents a optional function argument to NewClient
type Option func(*Client) error

// WithURL replaces the default URL when calling NewClient
func WithURL(url string) Option {
	return func(c *Client) error {
		_, err := urlpkg.Parse(url)
		if err != nil {
			return err
		}
		c.url = url
		return nil
	}
}

// WithHTTPClient replaces the default http client config when calling NewClient
func WithHTTPClient(client *http.Client) Option {
	return func(c *Client) error {
		if client == nil {
			return errors.New("invalid http client: client cannot be nil")
		}
		c.c = client
		return nil
	}
}

func WithConsumerGroup(group string) Option {
	return func(c *Client) error {
		c.consumerGroup = group
		return nil
	}
}

// Client is a lightweight client around the haraqa http api, use NewClient() to create a new client
type Client struct {
	c             *http.Client
	url           string
	consumerGroup string
	dialer        *websocket.Dialer
	closer        chan struct{}
}

// NewClient creates a new client instance. Any options given override the local defaults
func NewClient(opts ...Option) (*Client, error) {
	c := &Client{
		c: &http.Client{
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				DialContext: (&net.Dialer{
					Timeout:   30 * time.Second,
					KeepAlive: 30 * time.Second,
					DualStack: true,
				}).DialContext,
				ForceAttemptHTTP2:     true,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
				MaxIdleConns:          1000,
				MaxIdleConnsPerHost:   1000,
			},
		},
		url:           "http://127.0.0.1:4353",
		consumerGroup: "",
		dialer: &websocket.Dialer{
			Proxy:            http.ProxyFromEnvironment,
			HandshakeTimeout: 45 * time.Second,
		},
		closer: make(chan struct{}),
	}

	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, err
		}
	}

	return c, nil
}

// CreateTopic Creates a new topic. It returns an error if the topic already exists
func (c *Client) CreateTopic(topic string) error {
	req, err := http.NewRequest(http.MethodPut, c.url+"/topics/"+topic, nil)
	if err != nil {
		return err
	}

	resp, err := c.c.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusCreated {
		err = headers.ReadErrors(resp.Header)
		return errors.Wrap(err, "error creating topic")
	}
	return nil
}

// DeleteTopic Delete a topic
func (c *Client) DeleteTopic(topic string) error {
	req, err := http.NewRequest(http.MethodDelete, c.url+"/topics/"+topic, nil)
	if err != nil {
		return err
	}

	resp, err := c.c.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusNoContent {
		err = headers.ReadErrors(resp.Header)
		return errors.Wrap(err, "error deleting topic")
	}
	return nil
}

// ListTopics Lists all topics, filter by prefix, suffix, and/or a regex expression
func (c *Client) ListTopics(regex string) ([]string, error) {
	// check the regex expression
	if regex != "" {
		if _, err := regexp.Compile(regex); err != nil {
			return nil, err
		}
	}
	regex = urlpkg.QueryEscape(regex)
	path := c.url + "/topics?regex=" + regex
	resp, err := http.Get(path)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		err = headers.ReadErrors(resp.Header)
		return nil, errors.Wrap(err, "error getting topics")
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return strings.Split(string(body), ","), nil
}

// Produce sends messages from a reader to the designated topic
func (c *Client) Produce(topic string, sizes []int64, r io.Reader) error {
	req, err := http.NewRequest(http.MethodPost, c.url+"/topics/"+topic, r)
	if err != nil {
		return err
	}
	req.Header = headers.SetSizes(sizes, req.Header)

	resp, err := c.c.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		err = headers.ReadErrors(resp.Header)
		return errors.Wrap(err, "error producing")
	}
	return nil
}

// ProduceMsgs sends the messages to the designated topic
func (c *Client) ProduceMsgs(topic string, msgs ...[]byte) error {
	if len(msgs) == 0 {
		return nil
	}
	sizes := make([]int64, 0, len(msgs))
	for i := range msgs {
		if len(msgs[i]) > 0 {
			sizes = append(sizes, int64(len(msgs[i])))
		}
	}
	if len(sizes) == 0 {
		return nil
	}
	return c.Produce(topic, sizes, bytes.NewBuffer(bytes.Join(msgs, nil)))
}

var getRequestPool = &sync.Pool{
	New: func() interface{} {
		req, _ := http.NewRequest(http.MethodGet, "*", nil)
		return req
	},
}

// Consume reads messages off of a topic starting from id, no more than the given limit is returned.
// If limit is less than 1, the server sets the limit.
func (c *Client) Consume(topic string, id int64, limit int) (io.ReadCloser, []int64, error) {
	var err error
	req := getRequestPool.Get().(*http.Request)
	defer getRequestPool.Put(req)
	req.URL, err = url.Parse(c.url + "/topics/" + topic)
	if err != nil {
		return nil, nil, err
	}
	req.Header[headers.HeaderID] = []string{strconv.FormatInt(id, 10)}
	if limit > 0 {
		req.Header[headers.HeaderLimit] = []string{strconv.Itoa(limit)}
	}
	if c.consumerGroup != "" {
		req.Header[headers.HeaderConsumerGroup] = []string{c.consumerGroup}
	}

	resp, err := c.c.Do(req)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		err = headers.ReadErrors(resp.Header)
		if err == nil {
			err = errors.New("unexpected server response")
		}
		return nil, nil, errors.Wrap(err, "error consuming")
	}

	sizes, err := headers.ReadSizes(resp.Header)
	if err != nil {
		return nil, nil, err
	}

	return resp.Body, sizes, nil
}

// ConsumeMsgs reads messages off of a topic starting from id, no more than the given limit is returned.
// If limit is less than 1, the server sets the limit.
func (c *Client) ConsumeMsgs(topic string, id int64, limit int) ([][]byte, error) {
	r, sizes, err := c.Consume(topic, id, limit)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	msgs := make([][]byte, len(sizes))
	for i := range sizes {
		msgs[i] = make([]byte, sizes[i])
		_, err = io.ReadAtLeast(r, msgs[i], len(msgs[i]))
		if err != nil {
			return nil, err
		}
	}
	return msgs, nil
}

// WatchTopics opens a websocket to the server to listen for changes to the given topics.
// It writes the name of any modified topics to the given channel until a context cancellation or an error occurs
func (c *Client) WatchTopics(ctx context.Context, topics []string, ch chan<- string) error {
	if ch == nil {
		return errors.New("receiver channel cannot be nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if len(topics) == 0 {
		return headers.ErrInvalidTopic
	}
	for i := range topics {
		topics[i] = strings.ToLower(topics[i])
	}

	path := strings.Replace(c.url, "http", "ws", 1) + "/ws/topics"
	conn, resp, err := c.dialer.Dial(path, map[string][]string{
		headers.HeaderWatchTopics: topics,
	})
	if err != nil {
		return err
	}
	defer conn.Close()
	err = headers.ReadErrors(resp.Header)
	if err != nil {
		return err
	}

	errs := make(chan error, 1)
	go func() {
		for ctx.Err() == nil {
			msgType, b, err := conn.ReadMessage()
			if err != nil {
				errs <- err
				return
			}
			if msgType == websocket.TextMessage {
				ch <- string(b)
			}
		}
	}()

	for {
		select {
		case <-c.closer:
			_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "client closing"), time.Now().Add(time.Second*30))
			return nil
		case <-ctx.Done():
			_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "client closed on context"), time.Now().Add(time.Second*30))
			return ctx.Err()
		case err = <-errs:
			_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "client closing on error"), time.Now().Add(time.Second*30))
			return err
		}
	}
}

// Close closes any active topic watchers
func (c *Client) Close() error {
	if c.closer != nil {
		close(c.closer)
	}
	return nil
}

// NewWriter creates a new producer consistent with io.Writer
func (c *Client) NewWriter(topic string, delimiter []byte) io.Writer {
	return &Writer{
		c:     c,
		topic: topic,
		delim: delimiter,
	}
}

type Writer struct {
	c     *Client
	topic string
	delim []byte
}

// Write produces messages to the haraqa instance separated by the delimiter. This is thread safe
func (w *Writer) Write(b []byte) (int, error) {
	var err error
	if len(w.delim) > 0 {
		msgs := bytes.SplitAfter(b, w.delim)
		//remove last msg if blank
		if len(msgs) > 0 && len(msgs[len(msgs)-1]) == 0 {
			msgs = msgs[:len(msgs)-1]
		}
		err = w.c.ProduceMsgs(w.topic, msgs...)
	} else {
		err = w.c.ProduceMsgs(w.topic, b)
	}
	if err != nil {
		return 0, err
	}
	return len(b), err
}
