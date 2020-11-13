#!/bin/bash
# shellcheck disable=SC2012
if [[ -z $1 || -z $2 ]]; then
  echo 'Usage: ./evaluate.sh <experiment> <run>'
  exit 1
fi
dir="csvs/$1/run$2"
experimentPath="../logs/run$2/$1/*.log"
if [[ ! -e "csvs" ]]; then
  mkdir "csvs"
fi
if [[ ! -e "csvs/$1" ]]; then
  mkdir "csvs/$1"
fi
if [[ ! -e $dir ]]; then
  mkdir $dir
fi
nNodes=$(ls $experimentPath | wc -l)
echo "nodes: $nNodes"


# shellcheck disable=SC2126
echo "Broadcasts sent: $(grep "BroadcastApp" $experimentPath | grep "Sending" | wc -l)"
# shellcheck disable=SC2004
for i in $(seq 00 $(($nNodes - 1))); do
  # shellcheck disable=SC2126
  echo "node$i received: $(grep "BroadcastApp" "../logs/run$2/$1/node$i.log" | grep "Received" | wc -l)"
done

function generateChannelMetrics() {
  echo "Generating ChannelMetrics.csv"
  filename="$dir/ChannelMetrics.csv"
  echo "MsgOut,BytesOut,MsgIn,BytesIn" >$filename
  declare -a metrics
  i=0
  IFS='
  '
  # shellcheck disable=SC2013
  for x in $(cat $experimentPath | grep -o "ChannelMetrics.*" | cut -f2- -d:); do
    for k in $(echo "$x" | tr ";" "\n" | tr "{" "\n" | tr -d "}" | cut -f2- -d= | grep "[0-9]" | tr -d ","); do
      i=$((i + 1))
      metrics[i]=$k
    done
  done
  unset IFS
  for i in "${!metrics[@]}"; do
    result="${result}${metrics[i]},"
    if [[ $((i % 4)) == 0 ]]; then
      echo $result >>"$filename"
      result=""
    fi
  done
}

function calculateLatency() {
  # shellcheck disable=SC2207
  sent=($(echo "$1" | tr ":" "\n" ))
  # shellcheck disable=SC2207
  receive=($(echo "$2" | tr ":" "\n" ))
  # shellcheck disable=SC2004
  h=$(($((10#${receive[0]})) - $((10#${sent[0]}))))
  # shellcheck disable=SC2004
  m=$(($((10#${receive[1]}))- $((10#${sent[1]}))))
  # shellcheck disable=SC2004
  s=$(($((10#${receive[2]})) - $((10#${sent[2]}))))
  # shellcheck disable=SC2004
  ms=$(($((10#${receive[3]})) - $((10#${sent[3]}))))
  # shellcheck disable=SC2004
  echo $(($(($(($h * 60 + $m)) * 60 + $s)) * 60 + $ms))
}
function generateLatencyAndReliability() {
  echo "Generating reliability_latency.csv"
  echo "Reliability,Latency" >"$dir/reliabilityLatency.csv"
  # shellcheck disable=SC2207
  sentMids=($(cat $experimentPath | grep "BroadcastApp" | grep "Sending" | tr " " "\n" | grep '.\{36\}'))
  # shellcheck disable=SC2207
  sentLatency=($(cat $experimentPath | grep "BroadcastApp" | grep "Sending" | grep -o -P 'I.{0,13}' | grep : | tr -d "I["))
  # shellcheck disable=SC2207
  receivedMids=($(cat $experimentPath | grep "BroadcastApp" | grep "Received" | tr " " "\n" | grep '.\{36\}'))
  # shellcheck disable=SC2207
  receivedLatency=($(cat $experimentPath | grep "BroadcastApp" | grep "Received" | grep -o -P 'I.{0,13}' | grep : | tr -d "I["))
  for ((i = 0; i < ${#sentMids[@]}; i++)); do
    idx=0
    l=0
    for ((k = 0; k < ${#receivedMids[@]} && l < nNodes; k++)); do
      if [[ "${receivedMids[$k]}" == "${sentMids[$i]}" ]]; then
        l=$((l + 1))
        idx=$k
      fi
    done
    echo "$(($l  * 100/ nNodes)),$(calculateLatency "${sentLatency[$i]}" "${receivedLatency[$idx]}")" >>"$dir/reliabilityLatency.csv"
  done
}

generateChannelMetrics
generateLatencyAndReliability