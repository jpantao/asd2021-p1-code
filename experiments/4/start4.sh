#!/bin/sh

idx=$1
shift
java -D logFilename=logs/4/node$idx -cp asdProj.jar Main -conf config4.properties "$@" &> /proc/1/fd/1
