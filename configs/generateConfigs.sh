#!/bin/bash

for experiment in $(seq 24); do
	cp config.properties "config$experiment.properties"
done
