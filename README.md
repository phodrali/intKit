# Go Kafka mTLS Demonstration

This project demonstrates mutual TLS (mTLS) authentication between two Go services (a producer and a consumer) communicating via a Kafka broker.

## Prerequisites

*   **Docker and Docker Compose**: To run the Kafka broker. (Ensure Docker has sufficient disk space to pull images).
*   **Go**: To build and run the producer and consumer services (version 1.18 or higher recommended).
*   **OpenSSL**: For generating SSL certificates.
*   **Java Keytool**: For managing Java Keystores (usually part of a JDK installation, required by the certificate generation script for Kafka's JKS files).

## Project Structure

```
.
├── certs/
│   ├── generate_certs.sh       # Script to generate all necessary certificates
│   ├── ca.pem                  # CA certificate (generated)
│   ├── ca.key                  # CA private key (generated)
│   ├── server.keystore.jks     # Kafka broker keystore (generated)
│   ├── server.truststore.jks   # Kafka broker truststore (generated)
│   ├── client.keystore.jks     # Client keystore (generated)
│   ├── client.truststore.jks   # Client truststore (generated)
│   ├── client.crt              # Client certificate in PEM format (generated)
│   ├── client.key              # Client private key in PEM format (generated)
│   └── ... (other intermediate files)
├── consumer/
│   ├── consumer.go             # Go code for the Kafka consumer
│   ├── go.mod                  # Go module file for consumer
│   └── go.sum                  # Go module sum file for consumer
├── producer/
│   ├── producer.go             # Go code for the Kafka producer
│   ├── go.mod                  # Go module file for producer
│   └── go.sum                  # Go module sum file for producer
├── docker-compose.yml        # Docker Compose file for Kafka and Zookeeper
└── README.md                 # This file
```

## Setup and Execution Steps

### 1. Clone the Repository

```bash
git clone <repository-url>
cd <repository-name>
```

### 2. Generate SSL Certificates

The Kafka broker and clients require SSL certificates for mTLS.
The `generate_certs.sh` script automates this process.

**Important**: Ensure `keytool` (from a Java JDK) and `openssl` are in your PATH.

```bash
cd certs
bash generate_certs.sh
cd ..
```
This will create all necessary `.jks`, `.pem`, `.key`, and `.crt` files in the `certs` directory. The default password for all generated keystores and certificates is `testpassword`.

### 3. Start the Kafka Broker

The `docker-compose.yml` file defines the Kafka and Zookeeper services, configured for mTLS.

```bash
docker-compose up -d
```
Wait for a minute for Kafka to start up properly. You can check the logs:
```bash
docker-compose logs kafka
```
Look for messages indicating that the Kafka broker has started successfully.

### 4. Initialize Go Modules (if not already done)

If you haven't run `go mod tidy` during development or if `go.mod`/`go.sum` files are missing:
```bash
cd producer
go mod init github.com/myorg/producer # Or your preferred module path
go mod tidy
cd ../consumer
go mod init github.com/myorg/consumer # Or your preferred module path
go mod tidy
cd ..
```

### 5. Run the Kafka Consumer

Open a new terminal window/tab and navigate to the `consumer` directory:

```bash
cd consumer
go run consumer.go
```
The consumer will start and try to connect to Kafka. It will wait for messages on the `mtls-test-topic`.

### 6. Run the Kafka Producer

Open another new terminal window/tab and navigate to the `producer` directory:

```bash
cd producer
go run producer.go
```
The producer will connect to Kafka and send 10 messages to the `mtls-test-topic`. You should see log output indicating messages being sent.

### 7. Verify Communication

*   **Producer Terminal**: You should see logs like "Sent message: Key=..., Value=...".
*   **Consumer Terminal**: You should see logs like "Consumed message: Topic=..., Value=...".

This confirms that messages are being produced and consumed securely over mTLS.

### 8. Clean Up

After you're done testing:

*   Stop the consumer and producer (Ctrl+C in their respective terminals).
*   Shut down the Kafka broker:
    ```bash
    docker-compose down -v # -v removes the volumes
    ```

## How mTLS is Configured

*   **Kafka Broker (`docker-compose.yml`)**:
    *   `KAFKA_SSL_CLIENT_AUTH: required` enforces client certificate authentication.
    *   `KAFKA_SSL_KEYSTORE_LOCATION` and `KAFKA_SSL_TRUSTSTORE_LOCATION` point to the server's keystore and truststore (containing the CA cert) generated in `certs/`.
*   **Go Producer/Consumer (`producer/producer.go`, `consumer/consumer.go`)**:
    *   A custom `tls.Config` is created.
    *   `RootCAs`: Loaded with `ca.pem` to trust the Kafka server's certificate.
    *   `Certificates`: Loaded with the client's certificate (`client.crt`) and private key (`client.key`) for the server to authenticate the client.
    *   This `tls.Config` is used in the `kafka.Dialer`.

## Troubleshooting

*   **"no space left on device" for Docker**: Ensure your Docker environment has enough disk space. You might need to prune unused Docker images/volumes: `docker system prune -a`.
*   **Certificate Errors**:
    *   Double-check that all certificates were generated correctly and paths in the Go code and `docker-compose.yml` are accurate.
    *   Ensure the `COMMON_NAME_SERVER` in `generate_certs.sh` (default: `kafka`) matches the hostname Kafka uses to advertise itself if `InsecureSkipVerify` is not set to `true` in Go clients. For `localhost` testing, this is usually fine.
    *   Verify that `ca.pem` is in both client and server truststores (implicitly for server via `KAFKA_SSL_TRUSTSTORE_LOCATION` and explicitly loaded in Go clients).
*   **Connection Refused (Producer/Consumer)**:
    *   Ensure Kafka is running (`docker ps`).
    *   Check Kafka logs (`docker-compose logs kafka`) for errors.
    *   Verify `kafkaBroker` address in Go code (`localhost:9093`) matches the `KAFKA_ADVERTISED_LISTENERS` SSL port in `docker-compose.yml`.
*   **`keytool` or `openssl` not found**: Make sure these utilities are installed and in your system's PATH.

This README should provide a good starting point for anyone wanting to run and understand this mTLS demonstration.
