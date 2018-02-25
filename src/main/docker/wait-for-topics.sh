#!/usr/bin/env bash

CHECK_STATUS_HOST="kafka-subscription"
CHECK_STATUS_PORT="2000"
CHECK_STATUS_PATH="up"

until [ $(curl -H "Content-Type: application/json" -I http://$CHECK_STATUS_HOST:$CHECK_STATUS_PORT/$CHECK_STATUS_PATH 2>/dev/null | grep "200 OK" | wc -l) -gt 0 ]; do
      sleep 1
done
nohup sh -c "java $JAVA_OPTS -Djava.security.egd=file:/dev/./urandom -jar /app.jar"