#!/bin/bash

for experiment in $(seq 11 2 24); do
	echo "Generating plots for: $experiment"
	./evaluate.sh $experiment $1
	wait
done
