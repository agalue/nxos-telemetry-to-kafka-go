# Test

The `sender.go` program was designed to test the gRPC solution.

In order to use it, a Kafka broker is required. The `docker-compose.yaml` starts a single node Kafka cluster for you.

Then, the idea is to start the gRPC server, the gRPC client, and finally the sender application.