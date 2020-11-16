#!/bin/bash
start=$1
end=$2
nNodes=$3
run=$4
for experiment in $(seq $start $end); do
	echo "Running experiment: $experiment"
	./deploy.sh $experiment $nNodes $run
	sleep 320
done

echo "Finished running experiments"