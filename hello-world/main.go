package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
)

type MsgBody struct {
	Id    string `json:"id"`
	Title string `json:"title"`
}

func handler(request events.SQSEvent) (string, error) {
	fmt.Printf("*** Got %d messages\n", len(request.Records))

	count := 0
	c := make(chan MsgBody)
	var wg sync.WaitGroup

	for _, message := range request.Records {
		wg.Add(1)
		s := message.Body
		data := MsgBody{}
		err := json.Unmarshal([]byte(s), &data)
		if err != nil {
			return "xxxxxx Fail to unmarshal the message body.", err
		}

		go sendIntercom("", data, &wg)

		count++
	}

	// this function literal (also called 'anonymous function' or 'lambda expression' in other languages)
	// is useful because 'go' needs to prefix a function and we can save some space by not declaring a whole new function for this
	go func() {
		wg.Wait() // this blocks the goroutine until WaitGroup counter is zero
		close(c)  // Channels need to be closed, otherwise the below loop will go on forever
	}() // This calls itself

	// this shorthand loop is syntactic sugar for an endless loop that just waits for results to come in through the 'c' channel
	for msg := range c {
		fmt.Println(msg)
	}

	return fmt.Sprintf("Processed %d messages.", count), nil
}

func sendIntercom(url string, msg MsgBody, wg *sync.WaitGroup) {
	defer (*wg).Done()

	postBody, err := json.Marshal(msg)
	if err != nil {
		log.Fatalf("Failed to marshal the message body. Error: %s", err)
	}
	responseBody := bytes.NewBuffer(postBody)
	resp, err := http.Post(url, "application/json", responseBody)
	if err != nil {
		log.Fatalf("Failed to send the message to Intercom. Error: %s", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalln(err)
	}
	sb := string(body)
	log.Printf(sb)
}

func main() {
	lambda.Start(handler)
}
