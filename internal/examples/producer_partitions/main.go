package main

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	/*	topicPrefix := "mytopic"
		numPartitions := 5
		batchSize := 10

		client, err := haraqa.NewClient()
		check(err)
		defer client.Close()

		producer, err:=client.NewProducer(haraqa.WithTopic(topic))

		ch := make(chan haraqa.ProduceMsg, batchSize)
		for i := 0; i < numPartitions; i++ {
			go func(topic []byte) {
				ctx := context.Background()
				err = client.ProduceLoop(ctx, topic, ch)
				check(err)
			}([]byte(fmt.Sprintf("%s-%d", topicPrefix, i)))
		}

		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go sendFiftyMessages(&wg, ch)
		}
		wg.Wait()*/
}

/*
func sendFiftyMessages(wg *sync.WaitGroup, ch chan haraqa.ProduceMsg) {
	for i := 0; i < 50; i++ {
		msg := haraqa.NewProduceMsg([]byte(fmt.Sprintf("message number %d", i)))
		ch <- msg
		err := <-msg.Err
		check(err)
	}
	wg.Done()
}
*/
