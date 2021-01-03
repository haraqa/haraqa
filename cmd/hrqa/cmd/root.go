package cmd

import (
	"fmt"
	"os"

	"github.com/haraqa/haraqa"
	"github.com/spf13/cobra"
)

var cfgFile string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "hrqa",
	Short: "hrqa is a cli client for haraqa",
	Example: `  hrqa topic create -t hello
  hrqa topic list
  hrqa produce -t hello -m world
  hrqa consume -t hello
	hrqa load
  hrqa topic delete -t hello`,
	Long: `hrqa is a cli client for haraqa. It can be used to manage topics,
produce messages, consume messages, and load test a broker.`,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().BoolP("verbose", "v", false, "set log level to verbose")
	rootCmd.PersistentFlags().StringP("server", "s", "http://127.0.0.1:4353", "Server to produce to")
	rootCmd.PersistentFlags().StringP("group", "g", "", "Consumer group to use")
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

type verbose struct {
	ok bool
}

func newVerbose(cmd *cobra.Command) *verbose {
	for cmd.HasParent() {
		cmd = cmd.Parent()
	}
	ok, err := cmd.PersistentFlags().GetBool("verbose")
	must(err)
	return &verbose{ok: ok}
}

func (v *verbose) Println(a ...interface{}) {
	if !v.ok {
		return
	}
	fmt.Println(a...)
}

func (v *verbose) Printf(s string, a ...interface{}) {
	if !v.ok {
		return
	}
	fmt.Printf(s, a...)
}

func newConnection(cmd *cobra.Command, vfmt *verbose) *haraqa.Client {
	for cmd.HasParent() {
		cmd = cmd.Parent()
	}
	//	cmd := child.Parent()
	serverAddr, err := cmd.PersistentFlags().GetString("server")
	must(err)
	consumerGroup, err := cmd.PersistentFlags().GetString("group")
	must(err)

	vfmt.Printf("Connecting to %+v \n", serverAddr)
	client, err := haraqa.NewClient(haraqa.WithURL(serverAddr), haraqa.WithConsumerGroup(consumerGroup))
	if err != nil {
		fmt.Printf("Unable to connect to broker: %q\n", err.Error())
		os.Exit(1)
	}
	vfmt.Println("Client connection successful.")

	return client
}
