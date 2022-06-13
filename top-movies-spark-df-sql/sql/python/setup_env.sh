#!/bin/bash
source "utils/load_dataset.sh"
source "utils/parse_yaml.sh"

eval $(parse_yaml "config/config.yaml")
load_dataset ${dataset} ${local_dataset_path}

hdfs dfs -test -d ${hadoop_path}
if [ $? -eq 0 ]; then
  hdfs dfs -rm -r ${hadoop_path} &>/dev/null
fi

hdfs dfs -mkdir ${hadoop_path}
hdfs dfs -put ${local_dataset_path} ${hadoop_path}