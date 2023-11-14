package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var (
	bootstrapServers string
	topic            string
	group            string
)

func init() {
	flag.StringVar(&bootstrapServers, "bootstrap-servers", "", "Kafka bootstrap servers")
	flag.StringVar(&topic, "topic", "", "Topic name")
	flag.StringVar(&group, "group", "", "Consumer group name")
}

func handleOAuthBearerTokenRefreshEvent(client kafka.Handle, e kafka.OAuthBearerTokenRefresh) {
	fmt.Println("Token refresh")
	token, expiryMs, err := signer.GenerateAuthToken(context.TODO(), "ap-southeast-2")
	if err != nil {
		fmt.Fprintf(os.Stderr, "%% Token retrieval error: %v\n", err)
		client.SetOAuthBearerTokenFailure(err.Error())
	} else {
		oauthBearerToken := kafka.OAuthBearerToken{
			TokenValue: token,
			Expiration: time.UnixMilli(expiryMs),
			Principal:  "admin",
		}
		setTokenError := client.SetOAuthBearerToken(oauthBearerToken)
		if setTokenError != nil {
			fmt.Fprintf(os.Stderr, "%% Error setting token and extensions: %v\n", setTokenError)
			client.SetOAuthBearerTokenFailure(setTokenError.Error())
		}
	}
}

func main() {
	flag.Parse()
	if bootstrapServers == "" {
		panic("bootstrap-servers is required")
	}

	if topic == "" {
		panic("topic is required")
	}

	if group == "" {
		panic("group is required")
	}

	bootstrapServers := os.Args[1]
	topic := os.Args[2]

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        bootstrapServers,
		"security.protocol":        "SASL_PLAINTEXT",
		"sasl.mechanisms":          "OAUTHBEARER",
		"group.id":                 group,
		"session.timeout.ms":       6000,
		"auto.offset.reset":        "earliest",
		"enable.auto.offset.store": false,
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", c)

	err = c.SubscribeTopics([]string{topic}, nil)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to subscribe to topic: %s\n", topic)
		os.Exit(1)
	}

	run := true
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM)

	for run {
		select {
		case sig := <-signalChannel:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				fmt.Printf("%% Message on %s:\n%s\n",
					e.TopicPartition, string(e.Value))
				if e.Headers != nil {
					fmt.Printf("%% Headers: %v\n", e.Headers)
				}
				_, err := c.StoreMessage(e)
				if err != nil {
					fmt.Fprintf(os.Stderr, "%% Error storing offset after message %s:\n",
						e.TopicPartition)
				}
			case kafka.Error:
				// Errors should generally be considered
				// informational, the client will try to
				// automatically recover.
				// But in this example we choose to terminate
				// the application if all brokers are down.
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			case kafka.OAuthBearerTokenRefresh:
				handleOAuthBearerTokenRefreshEvent(c, e)
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}

	fmt.Printf("Closing consumer\n")
	c.Close()
}
