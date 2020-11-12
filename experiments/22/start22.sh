#!/bin/sh

idx=$1
shift
java -D logFilename=logs/22/node$idx -cp asdProj.jar Main -conf config22.properties "$@" &> /proc/1/fd/1
