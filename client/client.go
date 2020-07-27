package client

import (
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/haraqa/haraqa/protocol"
)

type Client struct {
	c   *http.Client
	url string
}

func NewClient(url string) *Client {
	return &Client{
		c:   http.DefaultClient,
		url: url,
	}
}

func (c *Client) CreateTopic(topic string) error {
	req, err := http.NewRequest("PUT", c.url+"/topics/"+topic, nil)
	if err != nil {
		return err
	}

	resp, err := c.c.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusCreated {
		return errors.New("error creating topic")
	}
	return nil
}

func (c *Client) Produce(topic string, sizes []int64, r io.Reader) error {
	req, err := http.NewRequest("POST", c.url+"/topics/"+topic, r)
	if err != nil {
		return err
	}
	req.Header.Set(protocol.SetSizes(sizes))

	resp, err := c.c.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return errors.New("error producing")
	}
	return nil
}

func (c *Client) Consume(topic string, id uint64, batchSize int) (io.ReadCloser, []int64, error) {
	req, err := http.NewRequest("GET", c.url+"/topics/"+topic+"/"+strconv.FormatUint(id, 10), nil)
	if err != nil {
		return nil, nil, err
	}
	if batchSize > 0 {
		req.Header.Set("X-BATCH-SIZE", strconv.Itoa(batchSize))
	}

	resp, err := c.c.Do(req)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode != http.StatusPartialContent {
		return nil, nil, errors.New("error consuming")
	}

	sizes, err := protocol.ReadSizes(resp.Header)
	if err != nil {
		return nil, nil, err
	}

	return resp.Body, sizes, nil
}
