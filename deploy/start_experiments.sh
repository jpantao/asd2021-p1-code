#!/bin/bash
start=$1
end=$2
nNodes=$3
for experiment in $(seq $start $end); do
	deploy.sh $experiment $nNodes
	echo $experiment 
	wait
done
