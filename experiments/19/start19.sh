#!/bin/sh

idx=$1
shift
java -D logFilename=logs/19/node$idx -cp asdProj.jar Main -conf config19.properties "$@" &> /proc/1/fd/1
