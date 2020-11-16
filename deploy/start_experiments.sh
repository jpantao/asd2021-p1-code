#!/bin/bash
start=$1
end=$2
nNodes=$3
for experiment in $(seq $start $end); do
	echo "Running $experiment "
	./deploy.sh $experiment $nNodes
	wait 320
done
