#!/bin/bash -e

function join { local IFS="$1"; shift; echo "$*"; }

IFS=$'\n'
OPTIONS=("acks=1")
for VAR in $(env)
do
  env_var=$(echo "$VAR" | cut -d= -f1)
  if [[ $env_var =~ ^KAFKA_ ]]; then
    key=$(echo "$env_var" | cut -d_ -f2- | tr '[:upper:]' '[:lower:]' | tr _ .)
    val=${!env_var}
    if [[ $key == "manager."* ]]; then
      echo "[Skipping] '$key'"
    else
      echo "[Configuring] '$key'='$val'"
      OPTIONS+=("$key=$val")
    fi
  fi
done

exec /nxos-grpc -opennms \
  -port ${PORT-50051} \
  -bootstrap ${BOOTSTRAP_SERVERS} \
  -topic ${TOPIC-OpenNMS.Sink.Telemetry-NXOS} \
  -max-buffer-size ${MESSAGE_BUFFER_SIZE-0} \
  -params $(join , ${OPTIONS[@]}) \
  -minion-location ${MINION_LOCATION} \
  -minion-id ${MINION_ID}
