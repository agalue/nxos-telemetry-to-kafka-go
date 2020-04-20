FROM golang:alpine AS builder
RUN mkdir /app && \
    echo "@edgecommunity http://nl.alpinelinux.org/alpine/edge/community" >> /etc/apk/repositories && \
    apk update && \
    apk add --no-cache build-base git librdkafka-dev@edgecommunity
ADD ./ /app/
WORKDIR /app
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -tags musl -a -o nxos-grpc .

FROM alpine
RUN echo "@edgecommunity http://nl.alpinelinux.org/alpine/edge/community" >> /etc/apk/repositories && \
    apk update && \
    apk add --no-cache bash librdkafka@edgecommunity && \
    rm -rf /var/cache/apk/* && \
    addgroup -S grpc && adduser -S -G grpc grpc
COPY --from=builder /app/nxos-grpc /nxos-grpc
COPY ./docker-entrypoint.sh /
USER grpc
LABEL maintainer="Alejandro Galue <agalue@opennms.org>" name="NX-OS gRPC to Kafka"
ENTRYPOINT [ "/docker-entrypoint.sh" ]
