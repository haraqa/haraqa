package logs_test

import (
	"log"
	"os"

	"github.com/haraqa/haraqa"
	"github.com/haraqa/haraqa/examples/logs"
)

func ExampleLogger() {
	logger := log.New(os.Stderr, "ERROR", log.LstdFlags)
	logErr, err := logs.NewLogger(logger, haraqa.DefaultConfig, []byte("Errors"))
	if err != nil {
		panic(err)
	}
	// Close should be called to flush any messages before exiting
	defer logErr.Close()

	logErr.Println("Some log here")
	logErr.Println("Another log here")
}
