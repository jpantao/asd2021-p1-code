#!/bin/bash
# shellcheck disable=SC2012
if [[ -z $1 ]]; then
  echo 'Usage: ./evaluate.sh <experiment>'
  exit 1
fi
dir="csvs/$1"
experimentPath="../logs/$1/*.log"
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
echo "Generating reliability.csv"
# shellcheck disable=SC2126
echo "Total:,$(grep "BroadcastApp" $experimentPath | grep "Sending" | wc -l)" >"$dir/reliability.csv"
# shellcheck disable=SC2004
for i in $(seq 00 $(($nNodes - 1))); do
  # shellcheck disable=SC2126
  echo "$i,$(grep "BroadcastApp" "../logs/$1/node$i.log" | grep "Received" | wc -l)" >>"$dir/reliability.csv"
done

function generateChannelMetrics() {
  echo "Generating ChannelMetrics.csv"
  filename="$dir/ChannelMetrics.csv"
  echo "InConnections,,,, OutConnections,,," >$filename
  echo "MsgOut,BytesOut,MsgIn,BytesIn,MsgOut,BytesOut,MsgIn,BytesIn" >>$filename
  declare -a metrics
  i=0
  IFS='
  '
  # shellcheck disable=SC2013
  # shellcheck disable=SC2207
  aux=($(cat $experimentPath | grep -o "ChannelMetrics.*" | cut -f2- -d:))
  for ((k = 0; k < ${#aux[@]} && l < nNodes; k += 1)); do
    for j in $(echo "${aux[$k]}" | tr ";" "\n" | tr "{" "\n" | tr -d "}" | cut -f2- -d= | grep "[0-9]" | tr -d ","); do
      i=$((i + 1))
      metrics[i]=$j
    done
  done
  unset IFS
  for i in "${!metrics[@]}"; do
    result="${result}${metrics[i]},"
    if [[ $((i % 8)) == 0 ]]; then
      echo $result >>"$filename"
      result=""
    fi
  done
}

function convertToMillis() {
  # shellcheck disable=SC2207
  a=($(echo "$1" | tr ":" "\n" | awk '{x=$0+0;print x}'))
  echo $(($(($((${a[0]} * 60 + ${a[1]})) * 60 + ${a[2]})) * 60 + ${a[3]}))
}
function generateLatency() {
  echo "Generating latency.csv"
  echo "SentTime, ReceivedTime" >"$dir/latency.csv"
  # shellcheck disable=SC2207
  sentMids=($(cat $experimentPath | grep "BroadcastApp" | grep "Sending" | sort | tr " " "\n" | grep '.\{36\}'))
  # shellcheck disable=SC2207
  sentLatency=($(cat $experimentPath | grep "BroadcastApp" | grep "Sending" | sort | grep -o -P 'I.{0,13}' | grep : | tr -d "I["))
  # shellcheck disable=SC2207
  receivedMids=($(cat $experimentPath | grep "BroadcastApp" | grep "Received" | sort | tr " " "\n" | grep '.\{36\}'))
  # shellcheck disable=SC2207
  receivedLatency=($(cat $experimentPath | grep "BroadcastApp" | grep "Received" | sort | grep -o -P 'I.{0,13}' | grep : | tr -d "I["))
  for ((i = 0; i < ${#sentMids[@]}; i += nNodes/2)); do
    l=0
    latencies="$(convertToMillis ${receivedLatency[$i]})"
    for ((k = 0; k < ${#receivedMids[@]} && l < nNodes; k++)); do
      if [[ "${receivedMids[$k]}" == "${sentMids[$i]}" ]]; then
        l=$((l + 1))
        latencies="${latencies},$(convertToMillis ${receivedLatency[$k]})"
      fi
    done
    echo $latencies >>"$dir/latency.csv"
  done
}

generateChannelMetrics
#generateLatency
