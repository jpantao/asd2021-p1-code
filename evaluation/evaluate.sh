#!/bin/bash
# shellcheck disable=SC2012
if [[ -z $1 || -z $2 || -z $3 || $1 != "eagerpushgossip" && $1 != "plumtree" || $2 != "cyclon" && $2 != "hyparview" ]]; then
  echo 'Usage: ./evaluate.sh <brodcast protocol> <membership protocol> <directory>'
  printf 'Broadcast protocols:\n\teagerpushgossip\n\tplumtree'
  echo
  printf 'Membership protocols:\n\thyparview\n\tcyclon'
  echo
  exit 1
fi
broadcast=$1
membership=$2
dir=$3
if [[ ! -e $dir ]]; then
    mkdir $dir
fi

nNodes=$(ls ../logs/*.log | wc -l)
echo "Generating reliability.csv"
# shellcheck disable=SC2126
echo "Total,$(grep "BroadcastApp" ../logs/*.log | grep "Sending" | wc -l)" >"$dir/reliability.csv"
# shellcheck disable=SC2004
for i in $(seq 00 $(($nNodes - 1))); do
  # shellcheck disable=SC2126
  echo "$i,$(grep "BroadcastApp" ../logs/node"$i".log | grep "Received" | wc -l)" >>"$dir/reliability.csv"
done

function generateMetrics() {
  echo "Generating $1"
  p=1
  if [ $1 = "channelMetrics.csv" ]; then
    echo "msgOut,bytesOut,msgIn,bytesIn" >"$dir/$1"
    p=4
  else
    m="totalEvents,messagesIn,messagesFailed,messagesSent,timers,notifications,requests,replies,customChannelEvents"
    if [ $1 = "broadcastProtocolMetrics.csv" ]; then
      if [ $3 = "plumtree" ]; then
        echo "eagerPush,lazyPushPeers,received,missing,lazyQueue,gossipTimers,${m}" >"$dir/$1"
        p=15
      fi
      if [ $3 = "eagerpushgossip" ]; then
        echo "neighbours,received,${m}" >"$dir/$1"
        p=11
      fi
    elif [ $1 = "membershipProtocolMetrics.csv" ]; then
      if [ $3 = "hyparview" ]; then
        echo "activeView,passiveView,totalEvents,${m}" >"$dir/$1"
        p=11
      fi
      if [ $3 = "cyclon" ]; then
        echo "neighbours,upConnections,pendingConnections,pendingMsgs,sample,${m}" >"$dir/$1"
        p=14
      fi
    fi
  fi
  declare -a metrics
  i=0
  IFS='
  '
  # shellcheck disable=SC2013
  for x in $(cat ../logs/*.log | grep -o "$2" | cut -f2- -d:); do
    for k in $(echo "$x" | tr ";" "\n" | tr "{" "\n" | tr -d "}" | cut -f2- -d= | grep "[0-9]" | tr -d ","); do
      i=$((i + 1))
      metrics[i]=$k
    done
  done
  unset IFS
  for i in "${!metrics[@]}"; do
    result="${result}${metrics[i]},"
    if [[ $((i % p)) == 0 ]]; then
      echo $result >>"$dir/$1"
      result=""
    fi
  done
}

function calculateLatency() {
  # shellcheck disable=SC2207
  s=($(echo "$1" | tr ":" "\n" | sed 's/^0*//'))
  # shellcheck disable=SC2207
  r=($(echo "$2" | tr ":" "\n" | sed 's/^0*//'))
  # shellcheck disable=SC2004
  h=$((${r[0]} - ${s[0]}))
  # shellcheck disable=SC2004
  m=$((${r[1]} - ${s[1]}))
  # shellcheck disable=SC2004
  s=$((${r[2]} - ${s[2]}))
  # shellcheck disable=SC2004
  ms=$((${r[3]} - ${s[3]}))
  # shellcheck disable=SC2004
  echo $(($(($(($h * 60 + $m)) * 60 + $s)) * 60 + $ms))
}
function generateLatency() {
  echo "Generating latency.csv"
  echo "sent,received,latency" >"$dir/latency.csv"
  # shellcheck disable=SC2207
  sentMids=($(cat ../logs/node*.log | grep "BroadcastApp" | grep "Sending" | tr " " "\n" | grep '.\{36\}'))
  # shellcheck disable=SC2207
  sentLatency=($(cat ../logs/node*.log | grep "BroadcastApp" | grep "Sending" | grep -o -P 'I.{0,13}' | grep : | tr -d "I["))
  # shellcheck disable=SC2207
  receivedMids=($(cat ../logs/node*.log | grep "BroadcastApp" | grep "Received" | tr " " "\n" | grep '.\{36\}'))
  # shellcheck disable=SC2207
  receivedLatency=($(cat ../logs/node*.log | grep "BroadcastApp" | grep "Received" | grep -o -P 'I.{0,13}' | grep : | tr -d "I["))
  for ((i = 0; i < ${#sentMids[@]}; i++)); do
    idx=0
    for ((k = 0, l = 0; k < ${#receivedMids[@]} && l < nNodes; k++)); do
      if [[ "${receivedMids[$k]}" == "${sentMids[$i]}" ]]; then
        l=$((l + 1))
        idx=$k
      fi
    done
    echo "${sentLatency[$i]},${receivedLatency[$idx]},$(calculateLatency "${sentLatency[$i]}" "${receivedLatency[$idx]}")" >>"$dir/latency.csv"
  done
}

generateMetrics channelMetrics.csv 'ChannelMetrics.*'
generateMetrics broadcastProtocolMetrics.csv 'BroadcastMetrics.*' $broadcast
generateMetrics membershipProtocolMetrics.csv 'MembershipMetrics.*' $membership
generateLatency
