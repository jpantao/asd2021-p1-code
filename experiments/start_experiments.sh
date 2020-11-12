#!/bin/bash
nNodes=$1
for experiment in $(seq 24); do
	deploy.sh $experiment $nNodes
done
