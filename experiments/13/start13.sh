#!/bin/sh

idx=$1
shift
java -D logFilename=logs/13/node$idx -cp asdProj.jar Main -conf config13.properties "$@" &> /proc/1/fd/1
