# nxos-telemetry-to-kafka-go

A gRPC Server implemented in Go to receive telemetry data from a Nexus switch and forward it to Kafka.

This has been designed to capture the data sent by the Cisco Nexus, wrap it inside a [SinkMessage](./api/sink.proto) GBP object in order to be able to split its content according to an optional parameter specifying the buffer size (`--max-buffer-size` which defaults to 0, meaning a single message will be sent).

When using the special flag `--opennms` is suplied, the source message will be wrapped into a [TelemetryMessage](./api/telemetry.proto) GBP object. In this case, it is mandatory to specify the Minion ID (`--minion-id`) and the Minion Location (`--minion-location`) in order to buile the Telemetry Message. Make sure the name of the target Kafka Topic (`--topic`) is compatible with what OpenNMS/Sentinel expect including the System ID (i.e. `OpenNMS.Sink.Telemetry`). This functionality is expected to be compatible with Horizon 24+ or Meridian 2019+.
