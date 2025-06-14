#!/bin/bash

# Variables
COUNTRY="US"
STATE="California"
LOCALITY="Mountain View"
ORGANIZATION="MyOrg"
ORGANIZATION_UNIT="MyOU"
COMMON_NAME_CA="MyCA"
COMMON_NAME_SERVER="kafka"
COMMON_NAME_CLIENT="client"
PASSWORD="testpassword"
VALIDITY=3650 # 10 years
KEY_SIZE=2048 # Adjusted from 4096 for keys for faster generation during testing, CA can be 4096

# Clean up previous certs
rm -f *.jks *.p12 *.pem *.cer *.csr *.key *.srl

# Create CA
echo "Generating CA..."
openssl genrsa -out ca.key 4096
openssl req -new -x509 -key ca.key -out ca.pem -days $VALIDITY -subj "/C=$COUNTRY/ST=$STATE/L=$LOCALITY/O=$ORGANIZATION/OU=$ORGANIZATION_UNIT/CN=$COMMON_NAME_CA" -passin pass:$PASSWORD -passout pass:$PASSWORD

# --- Server ---
echo "Generating Server key, CSR and Certificate..."
# Generate server private key
openssl genrsa -out server.key $KEY_SIZE

# Create server CSR
openssl req -new -key server.key -out server.csr -subj "/C=$COUNTRY/ST=$STATE/L=$LOCALITY/O=$ORGANIZATION/OU=$ORGANIZATION_UNIT/CN=$COMMON_NAME_SERVER" -passin pass:$PASSWORD

# Sign the server CSR with CA to get server.pem
openssl x509 -req -CA ca.pem -CAkey ca.key -in server.csr -out server.pem -days $VALIDITY -CAcreateserial -passin pass:$PASSWORD

echo "Generating Server Keystore (JKS)..."
# Create server PKCS12 file
openssl pkcs12 -export -in server.pem -inkey server.key -certfile ca.pem -name $COMMON_NAME_SERVER -out server.p12 -passout pass:$PASSWORD -passin pass:$PASSWORD
# Import server PKCS12 into JKS
keytool -importkeystore -srckeystore server.p12 -srcstoretype PKCS12 -destkeystore kafka.server.keystore.jks -storepass $PASSWORD -keypass $PASSWORD -srcstorepass $PASSWORD -noprompt -alias $COMMON_NAME_SERVER

# Import CA cert into Server Keystore as a trusted cert (if client auth is 'required', server needs to trust client CAs)
# This might not be strictly necessary for kafka.server.keystore.jks if it's only acting as a keystore,
# but often keystores also double as truststores for internal validation or more complex setups.
# The server's primary truststore for client certs is kafka.server.truststore.jks
keytool -importcert -alias CARoot -file ca.pem -keystore kafka.server.keystore.jks -storepass $PASSWORD -noprompt


echo "Generating Server Truststore (JKS)..."
# Server Truststore (for trusting clients or other CAs if needed)
keytool -importcert -alias CARoot -file ca.pem -keystore kafka.server.truststore.jks -storepass $PASSWORD -noprompt


# --- Client ---
echo "Generating Client key, CSR and Certificate..."
# Generate client private key
openssl genrsa -out client.key $KEY_SIZE

# Create client CSR
openssl req -new -key client.key -out client.csr -subj "/C=$COUNTRY/ST=$STATE/L=$LOCALITY/O=$ORGANIZATION/OU=$ORGANIZATION_UNIT/CN=$COMMON_NAME_CLIENT" -passin pass:$PASSWORD

# Sign the client CSR with CA to get client.pem
openssl x509 -req -CA ca.pem -CAkey ca.key -in client.csr -out client.pem -days $VALIDITY -CAcreateserial -passin pass:$PASSWORD # Use same .srl or manage separately

echo "Generating Client Keystore (JKS)..."
# Create client PKCS12 file
openssl pkcs12 -export -in client.pem -inkey client.key -certfile ca.pem -name $COMMON_NAME_CLIENT -out client.p12 -passout pass:$PASSWORD -passin pass:$PASSWORD
# Import client PKCS12 into JKS
keytool -importkeystore -srckeystore client.p12 -srcstoretype PKCS12 -destkeystore kafka.client.keystore.jks -storepass $PASSWORD -keypass $PASSWORD -srcstorepass $PASSWORD -noprompt -alias $COMMON_NAME_CLIENT

# Import CA cert into Client Keystore as a trusted cert
keytool -importcert -alias CARoot -file ca.pem -keystore kafka.client.keystore.jks -storepass $PASSWORD -noprompt


echo "Generating Client Truststore (JKS)..."
# Client Truststore (for trusting server cert)
keytool -importcert -alias CARoot -file ca.pem -keystore kafka.client.truststore.jks -storepass $PASSWORD -noprompt


# --- Create client PEM files (for Go clients or other non-JKS environments) ---
echo "Generating Client PEM files (client.key, client.crt, ca.pem)..."
# client.key is already generated
# client.pem is the client certificate, typically named .crt
cp client.pem client.crt
# ca.pem is already generated

# --- Create server PEM files (for reference or other non-JKS environments) ---
echo "Generating Server PEM files (server.key, server.crt, ca.pem)..."
# server.key is already generated
# server.pem is the server certificate, typically named .crt
cp server.pem server.crt
# ca.pem is already generated

# Clean up intermediate files
rm -f *.csr *.p12 # *.srl if you want to clean serial numbers

echo "Certificates generated successfully in certs/ directory."
echo "Key files: client.key, server.key, ca.key"
echo "Certificate files: client.crt (client.pem), server.crt (server.pem), ca.pem"
echo "JKS Keystores: kafka.client.keystore.jks, kafka.server.keystore.jks"
echo "JKS Truststores: kafka.client.truststore.jks, kafka.server.truststore.jks"
echo "Make sure to run this script from within the 'certs' directory."
echo "For example: cd certs && bash generate_certs.sh && cd .."
