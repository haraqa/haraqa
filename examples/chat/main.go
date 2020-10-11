package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/haraqa/haraqa"
)

const (
	colorReset  = "\033[0m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
)

func main() {
	var (
		bobUsername   = "bob@example.com"
		janeUsername  = "jane@example.com"
		chrisUsername = "chris@example.com"
	)

	topic := "chat:" + strings.Join([]string{bobUsername, janeUsername, chrisUsername}, ":")
	bob := NewUser(bobUsername, colorBlue)
	jane := NewUser(janeUsername, colorGreen)
	chris := NewUser(chrisUsername, colorYellow)

	go bob.Listen(topic)
	go jane.Listen(topic)
	go chris.Listen(topic)

	// bob and jane will send, all will receive
	bob.SendMsg(topic, "hello")
	jane.SendMsg(topic, "hi guys")

	// chris will disconnect
	chris.SendMsg(topic, "Hi, brb")
	chris.Leave(topic)

	// bob invites them to the party
	bob.SendMsg(topic, "Hey y'all, having a party tonight come over whenever")

	// jane responds and leaves
	jane.SendMsg(topic, "Awesome, see you there")
	jane.Leave(topic)

	// chris comes back after some time
	time.Sleep(time.Second * 2)
	fmt.Printf("\n")
	go chris.Listen(topic)
	chris.SendMsg(topic, "Just saw this, be there around 9")

	// chris and bob stop listening to the topic
	bob.Leave(topic)
	chris.Leave(topic)

	// one of them deletes the conversation
	time.Sleep(time.Second * 3)
	bob.DeleteConversation(topic)
}

type User struct {
	username string
	client   *haraqa.Client
	cancels  map[string]context.CancelFunc
	ids      map[string]int64
	color    string
}

func NewUser(username string, color string) *User {
	c, err := haraqa.NewClient()
	if err != nil {
		panic(err)
	}
	return &User{
		username: color + username + colorReset,
		client:   c,
		cancels:  map[string]context.CancelFunc{},
		ids:      map[string]int64{},
		color:    color,
	}
}

func (u *User) Listen(topic string) {
	// make sure topic is created
	err := u.client.CreateTopic(topic)
	if err != nil && !errors.Is(err, haraqa.ErrTopicAlreadyExists) {
		panic(err)
	}

	ch := make(chan string, 1)
	ch <- topic

	go func() {
		defer close(ch)
		ctx, cancel := context.WithCancel(context.TODO())
		u.cancels[topic] = cancel
		err := u.client.WatchTopics(ctx, []string{topic}, ch)

		// we can ignore context canceled errors from calling the cancel() function
		if err != nil && !errors.Is(err, context.Canceled) {
			panic(err)
		}
	}()

	fmt.Println(u.username, "started listening")
	defer fmt.Println("\n"+u.username, "stopped listening")

	if _, ok := u.ids[topic]; !ok {
		u.ids[topic] = 0
	}

	for topic = range ch {
		msgs, err := u.client.ConsumeMsgs(topic, u.ids[topic], -1)
		if err != nil && !errors.Is(err, haraqa.ErrNoContent) {
			panic(err)
		}
		for _, msg := range msgs {
			fmt.Println("\t", u.username, "RECEIVED:", string(msg))
		}
		u.ids[topic] += int64(len(msgs))
	}
}

func (u *User) SendMsg(topic, msg string) {
	// add in some color
	msg = u.color + msg + colorReset

	// we're simulating a chat, so give some time between messages
	time.Sleep(time.Second * 4)

	fmt.Println("\n"+u.username, "SENDING:", msg)
	err := u.client.ProduceMsgs(topic, []byte(msg))
	if err != nil {
		panic(err)
	}
}

func (u *User) DeleteConversation(topic string) {
	fmt.Println("\n"+u.username, "DELETING conversation")
	err := u.client.DeleteTopic(topic)
	if err != nil {
		panic(err)
	}
}

func (u *User) Leave(topic string) {
	// simulate user delay
	time.Sleep(time.Second * 1)

	// cancel the listener
	cancel, ok := u.cancels[topic]
	if ok && cancel != nil {
		cancel()
	}
}
