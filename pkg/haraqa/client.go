package haraqa

import (
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/haraqa/haraqa/internal/headers"
	"github.com/pkg/errors"
)

type Client struct {
	c   *http.Client
	url string
}

func NewClient(url string) *Client {
	return &Client{
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
		url: url,
	}
}

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

var getRequestPool = &sync.Pool{
	New: func() interface{} {
		req, _ := http.NewRequest(http.MethodGet, "*", nil)
		return req
	},
}

func (c *Client) Consume(topic string, id uint64, limit int) (io.ReadCloser, []int64, error) {
	var err error
	req := getRequestPool.Get().(*http.Request)
	defer getRequestPool.Put(req)
	req.URL, err = url.Parse(c.url + "/topics/" + topic + "?id=" + strconv.FormatUint(id, 10))
	if err != nil {
		return nil, nil, err
	}
	if limit > 0 {
		req.Header[headers.HeaderLimit] = []string{strconv.Itoa(limit)}
	}

	resp, err := c.c.Do(req)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode != http.StatusPartialContent {
		err = headers.ReadErrors(resp.Header)
		return nil, nil, errors.Wrap(err, "error consuming")
	}

	sizes, err := headers.ReadSizes(resp.Header)
	if err != nil {
		return nil, nil, err
	}

	return resp.Body, sizes, nil
}
