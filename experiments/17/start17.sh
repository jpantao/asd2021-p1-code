#!/bin/sh

idx=$1
shift
java -D logFilename=logs/17/node$idx -cp asdProj.jar Main -conf config17.properties "$@" &> /proc/1/fd/1
