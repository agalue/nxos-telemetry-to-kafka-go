# nxos-telemetry-to-kafka-go

The idea of this project is to implement in [Go](https://golang.org) a [gRPC](https://grpc.io/) server in order to stream telemetry statistics from a [Cisco Nexus](https://www.cisco.com/go/nexus) device, and then send these statistics to a topic in [Kafka](http://kafka.apache.org/).

The gRPC service definition for the Nexus devices is defined [here](https://github.com/CiscoDevNet/nx-telemetry-proto).

Even if the gRPC `gRPCMdtDialout` service is defined as bi-directional, this application focus only on the client streaming part; in other words, receiving data from the Nexus switch.

Using UDP is another way to obtain telemetry data from Cisco, but due to the physical size limitation of UDP (65,507 bytes according to [Wikipedia](https://en.wikipedia.org/wiki/User_Datagram_Protocol)), it it not possible to send large groups of telemetry data through UDP, due to the hierarchical structure of the data (obtained through `NX-API`, or data management engine `DME`), and the potential amount of resources involved on big switches. This is due to the fact that each UDP packet is independent of each other (it is self contained), and the switch won't split the data into multiple packets.

These limitations force the administrator of the switch to define hundreds if not thousands of sensor path entries on the telemetry configuration to guarantee that the amount of data fits the UDP packet.

For example,

```txt
telemetry
  destination-group 100
    ip address 192.168.0.253 port 50001 protocol UDP encoding GPB
  sensor-group 200
    path sys/intf/phys-[eth1/1]/dbgIfHCIn depth 0
    path sys/intf/phys-[eth1/1]/dbgIfHCOut depth 0
    path sys/intf/phys-[eth1/2]/dbgIfHCIn depth 0
    path sys/intf/phys-[eth1/2]/dbgIfHCOut depth 0
...
    path sys/intf/phys-[eth10/96]/dbgIfHCIn depth 0
    path sys/intf/phys-[eth10/96]/dbgIfHCOut depth 0
  subscription 300
    dst-grp 100
    snsr-grp 200 sample-interval 300000
```

Also, due to the MTU settings across the network infrastructure between the Nexus and the recipient (in this case Kafka), that also applies as a limitation for choosing what to send over UDP, even when using GPB as the encoding protocol (as the payload is smaller than JSON, besides another benefits).

Fortunately this is not a limitation when using gRPC as the transport protocol, and it is possible to send a huge section of the telemetry data on a single sensor definition, which simplifies the maintenance of the telemetry configuration on the switches.

For example,

```txt
telemetry
  destination-group 100
    ip address 192.168.0.253 port 50001 protocol gRPC encoding GPB
  sensor-group 200
    path sys/intf depth unbounded
  subscription 300
    dst-grp 100
    snsr-grp 200 sample-interval 300000
```

This tool has been designed to capture the data sent by the Cisco Nexus, wrap it inside a [SinkMessage](./api/sink.proto) GBP object in order to be able to split its content according to an optional parameter specifying the buffer size (`--max-buffer-size` which defaults to 0, meaning a single message will be sent).

When using the special flag `--opennms` is suplied, the source message will be wrapped into a [TelemetryMessage](./api/telemetry.proto) GBP object. In this case, it is mandatory to specify the Minion ID (`--minion-id`) and the Minion Location (`--minion-location`) in order to buile the Telemetry Message. Make sure the name of the target Kafka Topic (`--topic`) is compatible with what OpenNMS/Sentinel expect including the System ID (i.e. `OpenNMS.Sink.Telemetry`). This functionality is expected to be compatible with Horizon 24+ or Meridian 2019+.

The [client](./client) directory contains a sample Kafka consumer application that can process single or multi-part `SinkMessage` messages and parse the final content into a `TelemetryMessage`.

The [test](./test) directory contains a test application that emulates a Nexus switch (i.e. a gRPC client) that sends mock content as a valid `TelemetryMessage`.

## Compilation

This tool was designed and implemented with Go 1.13 using Go-Modules.

To compile for linux:

```bash
GOOS=linux go build -a -o nxos-grpc .
```

Alternatively, the provided `Dockerfile` offers a way to compile and create an image based on `alpine` and the latest version of `librdkafka`.

The `docker-entrypoint.sh` assumes `opennms` mode. For this reason, if OpenNMS is not going to be involved, make sure to update that file.

## Usage

```bash
Usage of nxos-grpc:
  -bootstrap string
      kafka bootstrap server (default "localhost:9092")
  -debug
      to display a human-readable version of the GBP paylod sent by the Nexus
  -max-buffer-size int
      maximum buffer size
  -minion-id string
      the ID of the minion to emulate [opennms mode only]
  -minion-location string
      the location of the minion to emulate [opennms mode only]
  -opennms
      to emulate an OpenNMS minion when sending messages to kafka
  -params string
      optional kafka producer parameters as a CSV of Key-Value pairs
  -port int
      port to listen for gRPC requests (default 50001)
  -topic string
      kafka topic that will receive the messages (default "telemetry-nxos")
```
