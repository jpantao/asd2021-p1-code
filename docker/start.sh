#!/bin/sh

idx=$1
shift
java -D logFilename=logs/node$idx -cp asdProj.jar Main -conf config.properties "$@" &> /proc/1/fd/1
