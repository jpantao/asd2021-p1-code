#!/bin/bash
nNodes=$1
for experiment in $(seq 2); do
	deploy.sh $experiment $nNodes
	wait
done
