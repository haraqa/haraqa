package cmd

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/haraqa/haraqa"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

// consumeCmd represents the consume command
var consumeCmd = &cobra.Command{
	Use:   "consume",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		vfmt := newVerbose(cmd)

		// ge† flags
		topic, err := cmd.Flags().GetString("topic")
		must(err)
		offset, err := cmd.Flags().GetInt64("offset")
		must(err)
		maxBatchSize, err := cmd.Flags().GetInt64("max")
		must(err)

		// setup client connection
		client := newConnection(cmd, vfmt)
		defer client.Close()

		// send consume message
		ctx := context.Background()
		resp := haraqa.ConsumeResponse{}

		vfmt.Printf("Consuming from the topic %q\n", topic)
		err = client.Consume(ctx, []byte(topic), offset, maxBatchSize, &resp)
		if err != nil {
			fmt.Printf("Unable to consume message(s) from %q: %q\n", topic, err.Error())
			os.Exit(1)
		}

		// print messages to stdout
		for resp.N() > 0 {
			vfmt.Printf("Consuming next message\n")
			msg, err := resp.Next()
			if err != nil && errors.Cause(err) != io.EOF {
				fmt.Printf("Unable to consume message responses from %q: %q\n", topic, err.Error())
				os.Exit(1)
			}
			if len(msg) > 0 {
				fmt.Println(string(msg))
			}
			if err != nil {
				vfmt.Printf("Got error %q\n", err.Error())
			}
		}
	},
}

func init() {
	consumeCmd.Flags().StringP(topicFlag())
	must(consumeCmd.MarkFlagRequired("topic"))
	consumeCmd.Flags().Int64P("offset", "o", -1, "offset to consume from, -1 for next message from the next available offset")
	consumeCmd.Flags().Int64P("max", "m", 100, "maximum number of messages to consume")
	rootCmd.AddCommand(consumeCmd)
}
