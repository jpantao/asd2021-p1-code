#!/bin/bash

jar="target/asdProj.jar"

nNodes=$1
shift
config=$1
shift

port=5000

echo "Cleaning logs..."
rm logs/*.log

echo "Launching $nNodes local nodes..."

java -DlogFilename=logs/node0 -jar $jar -conf $config port=$port "$@" 2>&1 | sed "s/^/[node0] /" &

sleep 1

# shellcheck disable=SC2004
for i in $(seq 01 $(($nNodes - 1))); do
    # shellcheck disable=SC2004
    java -DlogFilename=logs/node"$i" -jar $jar -conf $config contact=localhost:$port port=$(($port+$i)) "$@" 2>&1 | sed "s/^/[node$i] /" &
    sleep 1
done

echo ""
echo "Preparing..."
wait

echo ""
echo "Done... All processes finished."
echo ""
# shellcheck disable=SC2126
echo "Broadcasts sent: $(grep "BroadcastApp" test/logs/*.log | grep "Sending" | wc -l)"
# shellcheck disable=SC2004
for i in $(seq 00 $(($nNodes - 1))); do
  # shellcheck disable=SC2126
  echo "node$i received: $(grep "BroadcastApp" test/logs/node"$i".log | grep "Received" | wc -l)"
done
