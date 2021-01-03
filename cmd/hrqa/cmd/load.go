package cmd

/*
import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/haraqa/haraqa"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

// loadCmd represents the load command
var loadCmd = &cobra.Command{
	Use:     "load",
	Short:   "Load test a broker",
	Example: `  hrqa load`,
	Long:    `Load test a broker by spawning goroutines to send messages.`,
	Run: func(cmd *cobra.Command, args []string) {
		vfmt := newVerbose(cmd)

		// ge† flags
		num, err := cmd.Flags().GetInt("num")
		must(err)
		limit, err := cmd.Flags().GetInt("limit")
		must(err)
		topic, err := cmd.Flags().GetString("topic")
		must(err)
		typed, err := cmd.Flags().GetString("type")
		must(err)
		msgSize, err := cmd.Flags().GetInt("msgSize")
		must(err)
		duration, err := cmd.Flags().GetDuration("duration")
		must(err)
		ticker, err := cmd.Flags().GetDuration("ticker")
		must(err)

		switch strings.ToLower(typed) {
		case "producer", "consumer", "prodcon":
		default:
			fmt.Printf("Invalid type: %q, valid options are producer, consumer, or prodcon\n", strings.ToLower(typed))
			os.Exit(1)
		}
		var wg sync.WaitGroup
		for i := 0; i < num; i++ {
			tmpTopic := topic
			if topic == "" {
				id := uuid.Must(uuid.NewRandom())
				tmpTopic = id.String()
			}

			switch strings.ToLower(typed) {
			case "producer":
				wg.Add(1)
				go loadProducer(cmd, vfmt, &wg, tmpTopic, limit, msgSize, duration, ticker)
			case "consumer":
				wg.Add(1)
				go loadConsumer(cmd, vfmt, &wg, tmpTopic, limit, msgSize, duration, ticker)
			case "prodcon":
				wg.Add(2)
				go loadProducer(cmd, vfmt, &wg, tmpTopic, limit, msgSize, duration, ticker)
				go loadConsumer(cmd, vfmt, &wg, tmpTopic, limit, msgSize, duration, ticker)
			}
		}
		wg.Wait()
	},
}

func init() {
	topicLong, topicShort, topicDefault, _ := topicFlag()
	loadCmd.Flags().StringP(topicLong, topicShort, topicDefault, "topic to load, optional. A uuid is generated for each goroutine if not given")
	loadCmd.Flags().IntP("num", "n", 1, "number of goroutines to spawn")
	loadCmd.Flags().String("type", "prodcon", `type of loader, e.g. "producer", "consumer", or "prodcon" (produce and consume)`)
	loadCmd.Flags().IntP("limit", "l", 100, "maximum number of messages to produce/consume per consume call")
	loadCmd.Flags().Int("msgSize", 100, "message size to produce")
	loadCmd.Flags().Duration("duration", time.Second*30, "duration to run the loading for")
	loadCmd.Flags().Duration("ticker", 0, "duration between consuming/producing messages, defaults to 0ms (send/receive as fast as possible)")

	rootCmd.AddCommand(loadCmd)
}

func loadProducer(cmd *cobra.Command, vfmt *verbose, wg *sync.WaitGroup, topic string, batchSize int, msgSize int, duration, ticker time.Duration) {
	defer wg.Done()

	done := make(chan struct{})

	msgBuf := make([]byte, msgSize)
	// best effort read rand data into message
	_, _ = rand.Read(msgBuf[:])
	msg := make([]byte, base64.StdEncoding.EncodedLen(msgSize))
	base64.StdEncoding.Encode(msg, msgBuf)
	msg = msg[:msgSize]
	msg[msgSize-1] = '\n'

	// setup client connection
	client := newConnection(cmd, vfmt)
	defer client.Close()

	err := client.CreateTopic(topic)
	if err != nil && errors.Cause(err) != haraqa.ErrTopicExists {
		vfmt.Println("error creating topic", err.Error())
		return
	}
	var total int64
	vfmt.Printf("Producing to %s\n", topic)
	defer func() {
		vfmt.Printf("Finished producing to %s, total produced: %v\n", topic, total)
	}()

	producer, err := client.NewProducer(
		haraqa.WithTopic(topic),
		haraqa.WithIgnoreErrors(),
		haraqa.WithBatchSize(batchSize),
		haraqa.WithErrorHandler(func(msgs [][]byte, err error) {
			vfmt.Println(err)
		}),
	)
	if err != nil {
		vfmt.Println("error creating producer", err.Error())
		return
	}
	defer producer.Close()

	exit := time.NewTimer(duration)
	defer exit.Stop()

	var tick *time.Ticker
	if ticker != 0 {
		tick = time.NewTicker(ticker)
		defer tick.Stop()
	}

	for {
		if tick != nil {
			<-tick.C
		}

		select {
		case <-done:
			return
		case <-exit.C:
			return
		default:
		}
		_ = producer.Send(msg)
	}
}

func loadConsumer(cmd *cobra.Command, vfmt *verbose, wg *sync.WaitGroup, topic string, batchSize int, msgSize int, duration, ticker time.Duration) {
	defer wg.Done()

	// setup client connection
	client := newConnection(cmd, vfmt)
	defer client.Close()

	err := client.CreateTopic(topic)
	if err != nil && errors.Cause(err) != haraqa.ErrTopicExists {
		vfmt.Println("error creating topic", err.Error())
		return
	}

	var msgs [][]byte
	var id uint64

	vfmt.Printf("Consuming from %s\n", topic)
	defer func() {
		vfmt.Printf("Finished consuming from %s, total consumed: %v\n", topic, id)
	}()
	exit := time.NewTimer(duration)
	defer exit.Stop()

	var tick *time.Ticker
	if ticker != 0 {
		tick = time.NewTicker(ticker)
		defer tick.Stop()
	}

	for {
		if tick != nil {
			<-tick.C
		}
		select {
		case <-exit.C:
			return
		default:
		}
		msgs, err = client.ConsumeMsgs(topic, id, batchSize)
		if err != nil {
			vfmt.Printf("Client consume error %s", err.Error())
			return
		}
		id += uint64(len(msgs))
	}
}
*/
