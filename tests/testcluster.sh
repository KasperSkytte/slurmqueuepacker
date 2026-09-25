#!/usr/bin/env bash
# Stand up a throwaway slurmctld with a real cluster's node/partition topology,
# as an unprivileged user, on non-default ports. There is no slurmd: the nodes
# are declared as cloud nodes whose resume program does nothing, so a job that
# fits is really allocated to a node (state CONFIGURING, it never runs) and one
# that does not stays PENDING. That is enough to see the partitions and node
# job_submit chose, and to fill nodes up. scancel frees them again.
#
# This is how the plugin is tested end to end without touching a real cluster.
#
#   usage: tests/testcluster.sh start|stop|status
#          SLURM_CONF=/tmp/sqp-cluster/slurm.conf sbatch -n 8 --mem=64G --wrap=...
set -u
D=${SQP_TEST_DIR:-/tmp/sqp-cluster}
REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

start() {
  mkdir -p "$D"/{state,log,run}
  cat > "$D/slurm.conf" <<EOF
ClusterName=sqptest
SlurmctldHost=$(hostname -s)
SlurmUser=$(id -un)
SlurmctldPort=7817
SlurmdPort=7818
StateSaveLocation=$D/state
SlurmctldPidFile=$D/run/slurmctld.pid
SlurmctldLogFile=$D/log/slurmctld.log
SlurmdSpoolDir=$D/state/d
SlurmctldDebug=info
AuthType=auth/munge
CredType=cred/munge
ProctrackType=proctrack/linuxproc
TaskPlugin=task/none
SwitchType=switch/none
AccountingStorageType=accounting_storage/none
JobAcctGatherType=jobacct_gather/none
SelectType=select/cons_tres
SelectTypeParameters=CR_CPU_Memory
SchedulerType=sched/backfill
ReturnToService=2
MaxJobCount=10000
DefMemPerNode=512
EnforcePartLimits=ALL
JobSubmitPlugins=lua
# Cloud nodes, "resumed" by /bin/true and never suspended: allocation without slurmd.
SuspendProgram=/bin/true
ResumeProgram=/bin/true
ResumeTimeout=36000
SuspendTime=-1
SchedulerParameters=sched_min_interval=0
GresTypes=gpu

# Real biocloud topology. NodeAddr points at localhost because these nodes do
# not exist; without it slurmctld fails to resolve them at startup.
NodeName=bio-node01 NodeAddr=127.0.0.1 CPUs=256 RealMemory=1021540 State=CLOUD
NodeName=bio-node02 NodeAddr=127.0.0.1 CPUs=192 RealMemory=505529  State=CLOUD
NodeName=bio-node[03-07] NodeAddr=127.0.0.1 CPUs=192 RealMemory=1021567 State=CLOUD
NodeName=bio-node08 NodeAddr=127.0.0.1 CPUs=192 RealMemory=2041663 State=CLOUD
NodeName=bio-node09 NodeAddr=127.0.0.1 CPUs=256 RealMemory=2041636 State=CLOUD
NodeName=bio-node10 NodeAddr=127.0.0.1 CPUs=64  RealMemory=247474  Gres=gpu:a10:1 State=CLOUD
NodeName=bio-node11 NodeAddr=127.0.0.1 CPUs=288 RealMemory=1537338 State=CLOUD
NodeName=bio-node[12-13] NodeAddr=127.0.0.1 CPUs=288 RealMemory=1537338 State=CLOUD
NodeName=bio-node[14-15] NodeAddr=127.0.0.1 CPUs=288 RealMemory=2311479 State=CLOUD
NodeName=bio-node[16-17] NodeAddr=127.0.0.1 CPUs=256 RealMemory=1537407 State=CLOUD

PartitionName=DEFAULT MaxTime=14-00:00:00 DefaultTime=0-01:00:00 State=UP OverSubscribe=NO
PartitionName=interactive Nodes=bio-node11 PriorityTier=1 MaxTime=1-00:00:00
PartitionName=zen5  Nodes=bio-node[12-13],bio-node[16-17] PriorityTier=10 Default=YES
PartitionName=zen3  Nodes=bio-node[01-07] PriorityTier=9
PartitionName=zen5x Nodes=bio-node[14-15] PriorityTier=8
PartitionName=zen3x Nodes=bio-node[08-09] PriorityTier=7
PartitionName=gpu-a10 Nodes=bio-node10 PriorityTier=1
EOF

  sed -e "s#/run/sqp/policy.lua#$D/policy.lua#" -e "s#/etc/sqp/disable#$D/disable#" \
      "$REPO/lua/job_submit.lua" > "$D/job_submit.lua"

  cat > "$D/sqp.toml" <<EOF
[general]
mode = "advise"
state_dir = "$D"
log_file = "$D/decisions.jsonl"
disable_file = "$D/disable"
# Partitions are discovered: interactive is dropped by name, gpu-a10 because
# its only node has a GPU. Nothing is listed by hand.
[topology]
[topology.speed]
zen5 = 1.0
zen5x = 1.0
zen3 = 0.8
zen3x = 0.8
EOF

  slurmctld -f "$D/slurm.conf" -D >> "$D/log/ctld.out" 2>&1 &
  sleep 3
  SLURM_CONF="$D/slurm.conf" scontrol ping
  echo "config:  $D/slurm.conf"
  echo "run:     SLURM_CONF=$D/slurm.conf sbatch -n 8 --mem=64G -t 10 --wrap='sleep 60'"
  echo "daemon:  SLURM_CONF=$D/slurm.conf python3 -m sqp.daemon -c $D/sqp.toml"
}

stop() {
  [ -f "$D/run/slurmctld.pid" ] && kill "$(cat "$D/run/slurmctld.pid")" 2>/dev/null
  sleep 1; echo "stopped"
}

status() { SLURM_CONF="$D/slurm.conf" sinfo -h -o "%P %D %c %m %t"; }

case "${1:-start}" in
  start) start ;; stop) stop ;; status) status ;;
  *) echo "usage: $0 start|stop|status" >&2; exit 2 ;;
esac
