package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func topicFlag() (string, string, string, string) {
	return "topic", "t", "", "topic to create (required)"
}

// topicCmd represents the topic command
var topicCmd = &cobra.Command{
	Use:   "topic",
	Short: "Manage topics",
	Long:  `"hrqa topic" requires exactly 1 argument.`,
}

func init() {
	topicDeleteCmd.Flags().StringP(topicFlag())
	topicDeleteCmd.MarkFlagRequired("topic")

	topicCreateCmd.Flags().StringP(topicFlag())
	topicCreateCmd.MarkFlagRequired("topic")

	topicCmd.AddCommand(topicListCmd, topicCreateCmd, topicDeleteCmd)
	rootCmd.AddCommand(topicCmd)
}

// topicCreateCmd represents the create command
var topicCreateCmd = &cobra.Command{
	Use:     "create",
	Short:   "Create a new topic",
	Example: `  hrqa topic create -t hello`,
	Long:    `Create a new topic.`,
	Run: func(cmd *cobra.Command, args []string) {
		vfmt := NewVerbose(cmd)

		// ge† flags
		topic, err := cmd.Flags().GetString("topic")
		must(err)

		// setup client connection
		client := NewConnection(cmd, vfmt)
		defer client.Close()

		err = client.CreateTopic(context.Background(), []byte(topic))
		if err != nil {
			fmt.Printf("Unable to create topic %q: %q\n", topic, err.Error())
			os.Exit(1)
		}
	},
}

// topicDeleteCmd represents the delete command
var topicDeleteCmd = &cobra.Command{
	Use:     "delete",
	Short:   "Delete a topic",
	Example: `  hrqa topic delete -t hello`,
	Long:    `Delete a topic.`,
	Run: func(cmd *cobra.Command, args []string) {
		vfmt := NewVerbose(cmd)

		// ge† flags
		topic, err := cmd.Flags().GetString("topic")
		must(err)

		// setup client connection
		client := NewConnection(cmd, vfmt)
		defer client.Close()

		vfmt.Printf("Deleting topic %q\n", topic)
		err = client.DeleteTopic(context.Background(), []byte(topic))
		if err != nil {
			fmt.Printf("Unable to delete topic %q: %q\n", topic, err.Error())
			os.Exit(1)
		}
		vfmt.Printf("Deleted topic %q\n", topic)
	},
}

// topicListCmd represents the list command
var topicListCmd = &cobra.Command{
	Use:     "list",
	Short:   "List all topics",
	Example: `  hrqa topic list`,
	Long:    `List all topics.`,
	Run: func(cmd *cobra.Command, args []string) {
		vfmt := NewVerbose(cmd)

		// setup client connection
		client := NewConnection(cmd, vfmt)
		defer client.Close()

		topics, err := client.ListTopics(context.Background())
		if err != nil {
			fmt.Printf("Unable to list topics: %q\n", err.Error())
			os.Exit(1)
		}
		if len(topics) == 0 {
			fmt.Println("No topics found.")
			os.Exit(0)
		}
		fmt.Println("Found topics:")
		for i := range topics {
			fmt.Printf("\t%s\n", topics[i])
		}
	},
}
