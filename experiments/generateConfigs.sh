#!/bin/bash

for experiment in $(seq 24); do
	config>"./$experiment/start$experiment.sh"
done