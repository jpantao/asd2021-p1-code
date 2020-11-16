#!/bin/bash

for experiment in $(seq 2 2 24); do
	echo "Generating plots for: $experiment"
	./evaluate.sh $experiment
	wait
done
