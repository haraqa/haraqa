package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// consumeCmd represents the consume command
var consumeCmd = &cobra.Command{
	Use:   "consume",
	Short: "Consume messages from a topic",
	Long:  `Consume messages from a topic`,
	Run: func(cmd *cobra.Command, args []string) {
		vfmt := newVerbose(cmd)

		// ge† flags
		topic, err := cmd.Flags().GetString("topic")
		must(err)
		id, err := cmd.Flags().GetInt64("id")
		must(err)
		limit, err := cmd.Flags().GetInt("limit")
		must(err)
		follow, err := cmd.Flags().GetBool("follow")
		must(err)

		// setup client connection
		client := newConnection(cmd, vfmt)
		defer client.Close()

		if !follow {
			vfmt.Printf("Consuming from the topic %q\n", topic)
			msgs, err := client.ConsumeMsgs(topic, id, limit)
			if err != nil {
				fmt.Printf("Unable to consume message(s) from %q: %q\n", topic, err.Error())
				os.Exit(1)
			}

			// print messages to stdout
			for _, msg := range msgs {
				fmt.Println(string(msg))
			}
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ch := make(chan string, 1)
		go func() {
			defer close(ch)
			err := client.WatchTopics(ctx, []string{topic}, ch)
			if err != nil {
				fmt.Println("Watch topics error:", err)
			}
		}()

		for topic := range ch {
			vfmt.Printf("Consuming from the topic %q\n", topic)
			msgs, err := client.ConsumeMsgs(topic, id, limit)
			if err != nil {
				fmt.Printf("Unable to consume message(s) from %q: %q\n", topic, err.Error())
				os.Exit(1)
			}

			// print messages to stdout
			for _, msg := range msgs {
				fmt.Println(string(msg))
			}
		}
	},
}

func init() {
	consumeCmd.Flags().StringP(topicFlag())
	must(consumeCmd.MarkFlagRequired("topic"))
	consumeCmd.Flags().Int64("id", -1, "offset id to consume from, -1 for message from the last available message")
	consumeCmd.Flags().IntP("limit", "l", 100, "maximum number of messages to consume per consume call")
	consumeCmd.Flags().BoolP("follow", "f", false, "follow the topic, continuously consume until ctrl+c is called")
	rootCmd.AddCommand(consumeCmd)
}
