#!/bin/sh

idx=$1
shift
java -D logFilename=logs/20/node$idx -cp asdProj.jar Main -conf config20.properties "$@" &> /proc/1/fd/1
