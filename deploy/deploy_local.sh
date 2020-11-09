#!/bin/bash

jar="target/asdProj.jar"
config="config.properties"


nNodes=$1
port=5000
shift

java -DlogFilename=logs/node0 -jar $jar -conf $config port=$port "$@" 2>&1 | sed "s/^/[node0] /" &

sleep 1

# shellcheck disable=SC2004
for i in $(seq 01 $(($nNodes - 1))); do
    # shellcheck disable=SC2004
    java -DlogFilename=logs/node"$i" -jar $jar -conf $config contact=localhost:$port port=$(($port+$i)) "$@" 2>&1 | sed "s/^/[node$i] /" &
    sleep 1
done

wait
echo "Done!"