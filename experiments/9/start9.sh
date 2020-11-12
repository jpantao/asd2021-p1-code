#!/bin/sh

idx=$1
shift
java -D logFilename=logs/9/node$idx -cp asdProj.jar Main -conf config9.properties "$@" &> /proc/1/fd/1
