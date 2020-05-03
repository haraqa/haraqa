package haraqa

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"time"

	"github.com/pkg/errors"
)

// Option is used with NewClient to set client parameters and override defaults
type Option func(*Client) error

// WithAddr sets the host address of the broker for the client to call
func WithAddr(addr string) Option {
	return func(c *Client) error {
		if addr == "" {
			return errors.New("invalid host")
		}
		c.addr = addr
		return nil
	}
}

// WithGRPCPort overrides the default grpc port (4353) of the broker that
// the client connects to.
func WithGRPCPort(port int) Option {
	return func(c *Client) error {
		c.gRPCPort = port
		return nil
	}
}

// WithDataPort overrides the default data port (14353) of the broker that
// the client connects to.
func WithDataPort(port int) Option {
	return func(c *Client) error {
		c.dataPort = port
		return nil
	}
}

// WithAutoCreateTopics overrides the default client behavior (createTopics=true)
// which automatically creates new topics on the broker if they do not exist already.
func WithAutoCreateTopics(createTopics bool) Option {
	return func(c *Client) error {
		c.createTopics = createTopics
		return nil
	}
}

// WithTimeout sets the client timeout for grpc and data messages.
func WithTimeout(timeout time.Duration) Option {
	return func(c *Client) error {
		c.timeout = timeout
		return nil
	}
}

// WithKeepAlive sets the interval between ping data messages.
func WithKeepAlive(interval time.Duration) Option {
	return func(c *Client) error {
		if interval == 0 {
			return errors.New("invalid keepalive interval")
		}
		c.keepalive = interval
		return nil
	}
}

// WithAESGCM encrypts individual messages prior to publishing and decypts
// messages when consuming.
func WithAESGCM(aesKey [32]byte) Option {
	return func(c *Client) error {
		// error is always nil for len(aesKey) == 32
		ci, _ := aes.NewCipher(aesKey[:])

		gcm, err := cipher.NewGCM(ci)
		if err != nil {
			return err
		}
		nonceSize := gcm.NonceSize()

		c.preProcess = append(c.preProcess, func(msgs [][]byte) error {
			nonce := make([]byte, nonceSize)
			if _, err := rand.Read(nonce); err != nil {
				return err
			}
			for i := range msgs {
				msgs[i] = gcm.Seal(nonce, nonce, msgs[i], nil)
			}
			return nil
		})

		c.postProcess = append(c.postProcess, nil)
		copy(c.postProcess[1:], c.postProcess)
		c.postProcess[0] = func(msgs [][]byte) error {
			for i := range msgs {
				if len(msgs[i]) < nonceSize {
					return errors.New("invalid message size")
				}
				nonce, ciphertext := msgs[i][:nonceSize], msgs[i][nonceSize:]
				msgs[i], err = gcm.Open(msgs[i][:0], nonce, ciphertext, nil)
				if err != nil {
					return err
				}
			}
			return nil
		}

		return nil
	}
}

// WithRetries sets the number of times a client should retry to send a message if
// the client experiences a network error.
func WithRetries(n int) Option {
	return func(c *Client) error {
		if n < 0 {
			return errors.New("number of retries must be non-negative")
		}
		c.retries = n
		return nil
	}
}
