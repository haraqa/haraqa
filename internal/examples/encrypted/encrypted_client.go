package encrypted

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"

	"github.com/haraqa/haraqa"
)

// Client wraps the haraqa client in aes encryption
type Client struct {
	haraqa.Client
	gcm cipher.AEAD
}

// NewClient returns a new encypted client around the haraqa client
func NewClient(client haraqa.Client, aesKey [32]byte) (*Client, error) {
	c, err := aes.NewCipher(aesKey[:])
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(c)
	if err != nil {
		return nil, err
	}

	return &Client{
		Client: client,
		gcm:    gcm,
	}, nil
}

// Produce wraps the haraqa client Produce and encrypts outgoing messages
func (c *Client) Produce(ctx context.Context, topic []byte, msgs ...[]byte) error {
	enc := make([][]byte, len(msgs))
	for i := range msgs {
		nonce := make([]byte, c.gcm.NonceSize())
		if _, err := rand.Read(nonce); err != nil {
			return err
		}
		enc[i] = c.gcm.Seal(nonce, nonce, msgs[i], nil)
	}

	return c.Client.Produce(ctx, topic, enc...)
}

// ProduceLoop wraps the haraqa client ProduceLoop and encrypts outgoing messages
func (c *Client) ProduceLoop(ctx context.Context, topic []byte, ch chan haraqa.ProduceMsg) error {

	chEnc := make(chan haraqa.ProduceMsg, cap(ch))
	go func() {
		defer close(chEnc)
		for msg := range ch {
			nonce := make([]byte, c.gcm.NonceSize())
			if _, err := rand.Read(nonce); err != nil {
				msg.Err <- err
				return
			}
			msg.Msg = c.gcm.Seal(nonce, nonce, msg.Msg, nil)
			chEnc <- msg
		}
	}()

	return c.Client.ProduceLoop(ctx, topic, chEnc)
}

// Consume wraps the haraqa client Consume and decrypts incoming messages
func (c *Client) Consume(ctx context.Context, topic []byte, offset int64, limit int64, buf *haraqa.ConsumeBuffer) ([][]byte, error) {
	msgs, err := c.Client.Consume(ctx, topic, offset, limit, buf)
	if err != nil {
		return nil, err
	}
	nonceSize := c.gcm.NonceSize()

	output := make([][]byte, len(msgs))
	for i := range msgs {
		if len(msgs[i]) < nonceSize {
			return nil, errors.New("invalid message size")
		}

		nonce, ciphertext := msgs[i][:nonceSize], msgs[i][nonceSize:]
		output[i], err = c.gcm.Open(nil, nonce, ciphertext, nil)
		if err != nil {
			return nil, err
		}
	}
	return output, nil
}
