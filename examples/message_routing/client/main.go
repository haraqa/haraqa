package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
)

var clientID = "my_client_id"

func main() {
	buf := bytes.NewReader([]byte("my first message\nmy second message\nmy third message\n"))
	err := sendMessage(buf)
	if err != nil {
		panic(err)
	}

	// get the first message
	b, err := getMessages(0, 1)
	if err != nil {
		panic(err)
	}
	fmt.Print(string(b))

	// get up to 100 messages, starting with the second message
	b, err = getMessages(1, 100)
	if err != nil {
		panic(err)
	}
	fmt.Print(string(b))

	// remove the clientID topic
	err = deleteTopic()
	if err != nil {
		panic(err)
	}
}

func sendMessage(r io.Reader) error {
	resp, err := http.Post("http://localhost:8080/"+clientID, "text/plain", r)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.New("invalid status returned")
	}
	return nil
}

func getMessages(offset int, n int) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, "http://localhost:8080/"+clientID, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("OFFSET", strconv.Itoa(offset))
	req.Header.Add("MAX", strconv.Itoa(n))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("invalid status returned")
	}
	defer resp.Body.Close()
	return ioutil.ReadAll(resp.Body)
}

func deleteTopic() error {
	req, err := http.NewRequest(http.MethodDelete, "http://localhost:8080/"+clientID, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.New("invalid status returned")
	}
	return nil
}
