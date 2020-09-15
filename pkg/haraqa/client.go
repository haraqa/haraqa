package haraqa

import (
	"bytes"
	"io"
	"net"
	"net/http"
	"net/url"
	urlpkg "net/url"
	"strconv"
	"sync"
	"time"

	"github.com/haraqa/haraqa/internal/headers"
	"github.com/pkg/errors"
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

// Client is a lightweight client around the haraqa http api, use NewClient() to create a new client
type Client struct {
	c   *http.Client
	url string
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
		url: "http://127.0.0.1:4353",
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
	sizes := make([]int64, len(msgs))
	for i := range msgs {
		sizes[i] = int64(len(msgs))
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
func (c *Client) Consume(topic string, id uint64, limit int) (io.ReadCloser, []int64, error) {
	var err error
	req := getRequestPool.Get().(*http.Request)
	defer getRequestPool.Put(req)
	req.URL, err = url.Parse(c.url + "/topics/" + topic + "?id=" + strconv.FormatUint(id, 10))
	if err != nil {
		return nil, nil, err
	}
	if limit > 0 {
		req.URL.RawQuery += "&limit=" + strconv.Itoa(limit)
	}

	resp, err := c.c.Do(req)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		err = headers.ReadErrors(resp.Header)
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
func (c *Client) ConsumeMsgs(topic string, id uint64, limit int) ([][]byte, error) {
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
