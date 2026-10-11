#!/bin/bash
# Creates the orders topic and loads the samples once.
# Empty topic: load and verify. Already loaded: skip.

set -euo pipefail

BIN=/opt/kafka/bin
BOOTSTRAP=localhost:29092
TOPIC=orders
STREAM=$(mktemp)
trap 'rm -f "$STREAM"' EXIT

fail() { echo "ERROR: $*" >&2; exit 1; }

# Messages in the topic = sum of the end offsets of its partitions
message_count() {
  "$BIN/kafka-get-offsets.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" --time -1 |
    awk -F: '{ sum += $3 } END { print sum + 0 }'
}

shopt -s nullglob
files=(/samples/*.json)
[ "${#files[@]}" -gt 0 ] || fail "No sample files found in /samples."
# Skip blank lines only: malformed JSON stays in, so the job can reject it
awk 'NF' "${files[@]}" > "$STREAM"
expected=$(awk 'END { print NR }' "$STREAM")

"$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
  --topic "$TOPIC" --partitions 3 --replication-factor 1

count=$(message_count)
if [ "$count" -ge "$expected" ]; then
  echo "Samples already loaded ($count messages in '$TOPIC'). Skipping."
  exit 0
fi
[ "$count" -eq 0 ] ||
  fail "Topic '$TOPIC' has $count of $expected messages (partial load). Reset with: docker compose down -v"

echo "Loading $expected sample messages into '$TOPIC'..."
"$BIN/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" \
  --producer-property acks=all < "$STREAM" ||
  fail "Producer failed. Reset with: docker compose down -v"

count=$(message_count)
[ "$count" -eq "$expected" ] ||
  fail "Expected $expected messages, found $count. Reset with: docker compose down -v"
echo "Loaded and verified $count messages."
