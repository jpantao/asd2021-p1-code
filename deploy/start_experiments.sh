#!/bin/bash
start=$1
end=$2
nNodes=$3
for experiment in $(seq $start $end); do
	echo "Running $experiment "
	./deploy.sh $experiment $nNodes
	sleep 320
done
