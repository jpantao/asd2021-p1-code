#!/bin/sh
experiment=$1
idx=$2
user=$3
shift 3

logsfilename="logs/$experiment/node$idx"
java -DlogFilename="$logsfilename" -cp asdProj.jar Main -conf "config$experiment.properties" "$@" &>/proc/1/fd/1
