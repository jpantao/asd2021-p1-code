#!/bin/bash

for experiment in $(seq 24); do
	./evaluate.sh $experiment
	wait
done
