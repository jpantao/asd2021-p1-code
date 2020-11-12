#!/bin/sh
experiment=$1
idx=$2
logsfilename="logs/$experiment/node$idx"
shift 2
java -DlogFilename="$logsfilename" -cp asdProj.jar Main -conf "config$experiment.properties" "$@" &>/proc/1/fd/1
