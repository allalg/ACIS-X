#!/bin/bash
set -e

# Format KRaft Kafka storage directory if not already initialized
if [ ! -f /tmp/kraft-combined-logs/meta.properties ]; then
  echo "[ENTRYPOINT] Formatting KRaft Kafka storage..."
  mkdir -p /tmp/kraft-combined-logs
  /opt/kafka/bin/kafka-storage.sh format -t MkU3OEVBNTcwNTJENDM2Qk -c /opt/kafka/config/kraft/server.properties --ignore-formatted || true
fi

exec /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf
