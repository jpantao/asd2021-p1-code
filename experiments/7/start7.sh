#!/bin/sh

idx=$1
shift
java -D logFilename=logs/7/node$idx -cp asdProj.jar Main -conf config7.properties "$@" &> /proc/1/fd/1
