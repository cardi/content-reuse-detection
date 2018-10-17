#!/usr/bin/env bash

i=
hadoop_jar="isi.jar"

script_name=$(basename $BASH_SOURCE) # format run-XXXXXX.sh
script_name=${script_name#run-}      # strip "run-" prefix
script_name=${script_name%.sh}       # strip ".sh" suffix
t=$script_name
#echo $script_name

# setting up some logging stuff
## http://www.goodmami.org/2011/07/simple-logging-in-bash-scripts/
exec 3>&2 # logging stream (file descriptor 3) defaults to STDERR
verbosity=2 # default to show warnings
silent_lvl=0
err_lvl=1
wrn_lvl=2
inf_lvl=3
dbg_lvl=4

notify() { log $silent_lvl "NOTE: $1"; } # Always prints
error() { log $err_lvl "ERROR: $1"; }
warn() { log $wrn_lvl "WARNING: $1"; }
inf() { log $inf_lvl "INFO: $1"; } # "info" is already a command
debug() { log $dbg_lvl "DEBUG: $1"; }
log() {
    if [ $verbosity -ge $1 ]; then
        # Expand escaped characters, wrap at 70 chars, indent wrapped lines
        echo -e "$2" | fold -w70 -s | sed '2~1s/^/  /' >&3
    fi
}

usage() {
cat << EOF
usage: $0 options

OPTIONS:
  -h    show this message
  -f    run over one file
  -a    run over all files
  -o    recursively delete output directory

  -i    experiment number
  -v    set verbosity level

  -q    suppress warning messages
  -d    print debug messages

  -r    dry run (not implemented)
EOF
}

# process options
while getopts "hi:faov:dq" OPTION
do
  case $OPTION in
    h)
      usage
      exit 1
      ;;
    i)
      i=$OPTARG # TODO test if number
      ;;
    f)
      f=true
      ;;
    a)
      a=true
      ;;
    o)
      o=true
      ;;
    v)
      verbosity=v
      ;;
    d)
      verbosity=$dbg_lvl
      ;;
    q)
      verbosity=$err_lvl
      ;;
    \?)
      usage
      exit
      ;;
  esac
done

# sanity check options
if [ "$i" == "" ]
then
  error "Need to have an experiment number/name"
  exit
fi

if [ "$f" = "" ] && [ "$a" = "" ]
then
  error "must specify either -f or -a"
  exit
fi

if [ "$f" = "true" ] && [ "$a" = "true" ]
then
  error "must specify only one of -f or -a" 1>&2
  exit
fi

# erasing output directory?
hdfs_output=/user/calvin/output-cc-$t-$i
test_hdfs_output=`hadoop fs -test -d $hdfs_output 2>/dev/null`

if [[ $? == 0 ]]; then
  if [ "$o" != "true" ]; then
    error "$hdfs_output already exists! use -o if you want to overwrite"
    exit
  elif [ "$o" == "true" ]; then
    warn "deleting $hdfs_output..."
    result=`hadoop fs -rm -r $hdfs_output`
    warn "$result"
  fi
elif [[ $? == 1 ]]; then
  inf "no hdfs output directory found, will be creating"
fi

# check hdfs input
hdfs_input=

if [ "$f" == "true" ]; then
  inf "using one file"
  hdfs_input=/user/calvin/cc/1285406207431_41.arc.gz
elif [ "$a" == "true" ]; then
  inf "using entire directory"
  hdfs_input=/user/calvin/cc
else
  error "should never get here!"
  exit 1
fi

# run hadoop jar
inf "running $hadoop_jar isi.$t $hdfs_input $hdfs_output"
hadoop jar $hadoop_jar \
  isi.$t \
  $hdfs_input \
  $hdfs_output
