#!/bin/bash
source "utils/load_dataset.sh"
source "utils/parse_yaml.sh"

# Get config arguments and load required dataset
eval $(parse_yaml "config/config.yaml")
load_dataset ${dataset} ${local_dataset_path}

# Remove hdfs directory if it exists
hdfs dfs -test -d ${hdfs_path}
if [ $? -eq 0 ]; then
  hdfs dfs -rm -r ${hdfs_path} &>/dev/null
fi

# Create new hdfs directory and upload dataset there
hdfs dfs -mkdir ${hdfs_path}
hdfs dfs -put ${local_dataset_path} ${hdfs_path}