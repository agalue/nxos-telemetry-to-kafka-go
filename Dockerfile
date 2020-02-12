FROM golang:alpine AS builder
RUN mkdir /app && apk add --no-cache build-base git librdkafka-dev
ADD ./ /app/
WORKDIR /app
RUN go build -a -o nxos-grpc .

FROM alpine
RUN apk add --no-cache build-base git librdkafka && rm -rf /var/cache/apk/*
COPY --from=builder /app/nxos-grpc /nxos-grpc
COPY ./docker-entrypoint.sh /
RUN addgroup -S grpc && adduser -S -G grpc grpc && apk add --no-cache bash
USER grpc
LABEL maintainer="Alejandro Galue <agalue@opennms.org>" name="NX-OS gRPC to Kafka"
ENTRYPOINT [ "/docker-entrypoint.sh" ]
