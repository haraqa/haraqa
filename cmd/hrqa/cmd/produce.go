package cmd

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
)

// produceCmd represents the produce command
var produceCmd = &cobra.Command{
	Use:   "produce",
	Short: "Produce one or more messages to the broker",
	Example: `  hrqa produce -t hello -m there -m world
  echo -e "there\nworld" | hrqa produce -t hello`,
	Long: `Produce one or more messages to the haraqa broker.`,
	Run: func(cmd *cobra.Command, args []string) {
		vfmt := newVerbose(cmd)

		// ge† flags
		topic, err := cmd.Flags().GetString("topic")
		must(err)
		msgs, err := cmd.Flags().GetStringSlice("msg")
		must(err)

		// setup client connection
		client := newConnection(cmd, vfmt)
		defer client.Close()

		// send messages if any are given
		if len(msgs) > 0 {
			vfmt.Printf("Producing message batch %+v\n", msgs)
			msgsBytes := make([][]byte, len(msgs))
			for i := range msgs {
				msgsBytes[i] = []byte(msgs[i])
			}
			err = client.ProduceMsgs(topic, msgsBytes...)
			if err != nil {
				fmt.Printf("Unable to produce message(s) %q: %q\n", msgs, err.Error())
				os.Exit(1)
			}
			vfmt.Println("Success")
			return
		}

		// handle ctrl+c
		c := make(chan os.Signal, 2)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		go func() {
			<-c
			client.Close()
			os.Exit(0)
		}()

		// send messages from stdin
		vfmt.Println("Reading messages from stdin")
		fmt.Println("Connected! Ready to send messages...")
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			msg := scanner.Bytes()
			if len(msg) == 0 {
				continue
			}
			vfmt.Printf("Producing message %q\n", string(msg))
			err = client.ProduceMsgs(topic, msg)
			if err != nil {
				fmt.Printf("Unable to produce message %q: %q\n", string(msg), err.Error())
				client.Close()
				os.Exit(1)
			}
			vfmt.Println("Success")
		}
		if err := scanner.Err(); err != nil {
			fmt.Printf("Error reading input: %q\n", err.Error())
			client.Close()
			os.Exit(1)
		}
		vfmt.Println("Done.")
	},
}

func init() {
	produceCmd.Flags().StringP(topicFlag())
	must(produceCmd.MarkFlagRequired("topic"))
	produceCmd.Flags().StringSliceP("msg", "m", []string{}, "message to send (optional, stdin is used if not provided)")
	rootCmd.AddCommand(produceCmd)
}
