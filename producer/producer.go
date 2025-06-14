package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	// "os" // Removed as log.Fatalf handles exit, and no other os functions used
	"time"

	"github.com/segmentio/kafka-go"
	// "github.com/segmentio/kafka-go/sasl/scram" // Already commented, but ensures it's not accidentally active
)

const (
	kafkaBroker = "localhost:9093" // Kafka broker address for SSL
	topic       = "mtls-test-topic"
	caCertPath  = "../certs/ca.pem"
	clientCertPath = "../certs/client.crt" // Corrected to .crt
	clientKeyPath  = "../certs/client.key" // Corrected to .key
)

func newKafkaWriter(topic string) (*kafka.Writer, error) {
	// Load CA cert
	caCert, err := ioutil.ReadFile(caCertPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA certificate: %w", err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// Load client cert and key
	clientCert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load client key pair: %w", err)
	}

	tlsConfig := &tls.Config{
		RootCAs:      caCertPool,
		Certificates: []tls.Certificate{clientCert},
		// InsecureSkipVerify: true, // Set to true if server hostname doesn't match CN in cert
	}

	dialer := &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		TLS:           tlsConfig,
	}

	// Check if SASL/SCRAM is enabled (it is not in this example, but good for future reference)
	// For mTLS only, SASL is not strictly required if Kafka is configured for SSL client auth.
	// If you had SASL/SCRAM:
	// mechanism, err := scram.Mechanism(scram.SHA256, "username", "password")
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to create SCRAM mechanism: %w", err)
	// }
	// dialer.SASLMechanism = mechanism


	w := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
		Transport: &kafka.Transport{
			Dial: dialer.DialFunc,
			TLS:  tlsConfig,
		},
		// RequiredAcks: kafka.RequireAll, // Or kafka.RequireOne, kafka.RequireNone
	}
	return w, nil
}

func main() {
	log.Println("Starting Kafka producer...")

	writer, err := newKafkaWriter(topic)
	if err != nil {
		log.Fatalf("Failed to create Kafka writer: %v", err)
	}
	defer writer.Close()

	log.Printf("Successfully connected to Kafka broker at %s", kafkaBroker)

	for i := 0; i < 10; i++ {
		msg := kafka.Message{
			Key:   []byte(fmt.Sprintf("Key-%d", i)),
			Value: []byte(fmt.Sprintf("Hello Kafka via mTLS! Message #%d", i)),
		}
		err := writer.WriteMessages(context.Background(), msg)
		if err != nil {
			log.Printf("Failed to write message %d: %v", i, err)
			// Attempt to re-establish connection or handle error
			newWriter, errReconnect := newKafkaWriter(topic)
			if errReconnect != nil {
				log.Fatalf("Failed to reconnect Kafka writer: %v", errReconnect)
			}
			writer.Close() // Close the old writer
			writer = newWriter // Assign the new writer

			// Retry sending the message
			errRetry := writer.WriteMessages(context.Background(), msg)
			if errRetry != nil {
				log.Fatalf("Failed to write message %d after reconnect: %v", i, errRetry)
			} else {
				log.Printf("Successfully sent message %d after reconnect", i)
			}
		} else {
			log.Printf("Sent message: Key=%s, Value=%s", string(msg.Key), string(msg.Value))
		}
		time.Sleep(1 * time.Second)
	}

	log.Println("Finished sending messages.")
}
