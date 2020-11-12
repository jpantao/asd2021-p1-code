#!/bin/sh

idx=$1
shift
java -D logFilename=logs/21/node$idx -cp asdProj.jar Main -conf config21.properties "$@" &> /proc/1/fd/1
