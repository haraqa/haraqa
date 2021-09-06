package main

import (
	"io"
	"log"
	"os"
	"runtime/debug"

	"github.com/haraqa/haraqa"
)

func main() {
	c, err := haraqa.NewClient()
	if err != nil {
		panic(err)
	}
	if err = c.CreateTopic("example_errors"); err != nil {
		panic(err)
	}
	if err = c.CreateTopic("example_warn"); err != nil {
		panic(err)
	}
	errLogger := log.New(
		c.NewWriter("example_errors", []byte{'\n'}),
		"[error] ",
		log.LstdFlags)
	warnLogger := log.New(
		c.NewWriter("example_warn", []byte{'\n'}),
		"[warn] ",
		log.LstdFlags)

	mw := io.MultiWriter(os.Stderr, c.NewWriter("example_errors", []byte{'\n'}))
	stdErrLogger := log.New(
		mw,
		"[error] ",
		log.LstdFlags)

	errLogger.Println("this is an error")
	stdErrLogger.Println("this is an error that also goes to std err")
	warnLogger.Println("this is a warning")
	warnLogger.Printf("this is a multi line warning %s\n", debug.Stack())
}
