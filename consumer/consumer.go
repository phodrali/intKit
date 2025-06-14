package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	// "github.com/segmentio/kafka-go/sasl/scram" // Uncomment if using SCRAM
)

const (
	kafkaBroker  = "localhost:9093" // Kafka broker address for SSL
	topic        = "mtls-test-topic"
	groupID      = "mtls-consumer-group"
	caCertPath   = "../certs/ca.pem"
	clientCertPath = "../certs/client.crt" // Corrected to .crt
	clientKeyPath  = "../certs/client.key" // Corrected to .key
)

func newKafkaReader(topic, groupID string) (*kafka.Reader, error) {
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
		Timeout:   10 * time.Second,
		DualStack: true,
		TLS:       tlsConfig,
	}

	// If you had SASL/SCRAM:
	// mechanism, err := scram.Mechanism(scram.SHA256, "username", "password")
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to create SCRAM mechanism: %w", err)
	// }
	// dialer.SASLMechanism = mechanism


	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		GroupID: groupID,
		Topic:   topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
		Dialer:   dialer,
	})
	return r, nil
}

func main() {
	log.Println("Starting Kafka consumer...")

	reader, err := newKafkaReader(topic, groupID)
	if err != nil {
		log.Fatalf("Failed to create Kafka reader: %v", err)
	}
	defer reader.Close()

	log.Printf("Successfully connected to Kafka broker at %s, consuming topic %s with group ID %s", kafkaBroker, topic, groupID)

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-signals
		log.Println("Interrupt is detected, shutting down consumer...")
		cancel()
	}()

	for {
		select {
		case <-ctx.Done():
			log.Println("Consumer shutting down.")
			return
		default:
			m, err := reader.FetchMessage(ctx)
			if err != nil {
				// Check if the context was cancelled, meaning we are shutting down
				if ctx.Err() != nil {
					return // Exit loop if context is cancelled
				}
				log.Printf("Error fetching message: %v. Re-initializing reader...", err)
				// Potentially re-initialize reader or handle specific errors
				reader.Close() // Close the old reader
				newR, initErr := newKafkaReader(topic, groupID)
				if initErr != nil {
					log.Fatalf("Failed to re-initialize Kafka reader: %v. Exiting.", initErr)
				}
				reader = newR // Assign the new reader
				continue // Try fetching message again in the next iteration
			}
			log.Printf("Consumed message: Topic=%s, Partition=%d, Offset=%d, Key=%s, Value=%s",
				m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))

			// Commit messages manually if auto-commit is disabled or you need specific commit logic
			if err := reader.CommitMessages(ctx, m); err != nil {
				log.Printf("Failed to commit message: %v", err)
			}
		}
	}
}
