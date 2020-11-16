run=$3
nNodes=$2
experiment=$1
shift 3

n_nodes=$(uniq $OAR_FILE_NODES | wc -l)

function nextnode {
  local idx=$(($1 % n_nodes))
  local i=0
  for host in $(uniq $OAR_FILE_NODES); do
    if [ $i -eq $idx ]; then
      echo $host
      break;
    fi
    i=$(($i +1))
  done
}

user=$(whoami)
mkdir ~/asdLogs

echo "Executing java"

printf "%.2d.. " 0

node=$(nextnode 0)
oarsh -n $node docker exec -d node-00 ./start.sh $experiment 0 $user $run "$@"

sleep 1

for i in $(seq 01 $(($nNodes - 1))); do
  node=$(nextnode $i)
  ii=$(printf "%.2d" $i)
  echo -n "$ii.. "
  if [ $((($i + 1) % 10)) -eq 0 ]; then
    echo ""
  fi
  oarsh -n $node docker exec -d node-${ii} ./start.sh $experiment $i $user $run contact=node-00:10000 "$@"
  sleep 0.5
done

echo ""
