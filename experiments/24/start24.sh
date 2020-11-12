#!/bin/sh

idx=$1
shift
java -D logFilename=logs/24/node$idx -cp asdProj.jar Main -conf config24.properties "$@" &> /proc/1/fd/1
