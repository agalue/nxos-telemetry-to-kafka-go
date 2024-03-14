FROM golang:alpine AS builder
RUN mkdir /app && \
    echo "@edgecommunity http://nl.alpinelinux.org/alpine/edge/community" >> /etc/apk/repositories && \
    apk update && \
    apk add --no-cache alpine-sdk git cyrus-sasl-dev librdkafka-dev@edgecommunity
ADD ./ /app/
WORKDIR /app
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -tags static_all,netgo,musl -o nxos-grpc .

FROM alpine
RUN apk add --no-cache bash tzdata && addgroup -S grpc && adduser -S -G grpc grpc
COPY --from=builder /app/nxos-grpc /nxos-grpc
COPY ./docker-entrypoint.sh /
USER grpc
LABEL maintainer="Alejandro Galue <agalue@opennms.org>" name="NX-OS gRPC to Kafka"
ENTRYPOINT [ "/docker-entrypoint.sh" ]
