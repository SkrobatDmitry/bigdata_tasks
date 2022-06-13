#!/bin/bash

# Get full path to the folder of executable script
full_path=""
if [[ ${BASH_SOURCE[0]} == *"/"* ]]; then
  full_path="$(cd "$(echo "${BASH_SOURCE[0]%/*}")"; pwd)/"
fi

# Get config arguments
source "${full_path}utils/parse_yaml.sh"
eval $(parse_yaml "${full_path}config/config.yaml")

# Remove hdfs directory if it exists
hdfs dfs -test -d ${hadoop_folder}
if [ $? -eq 0 ]; then
  hdfs dfs -rm -r ${hadoop_folder} &>/dev/null
fi

# Creates new hdfs directories and uploads dataset there
hdfs dfs -mkdir ${hadoop_folder}
hdfs dfs -put ${full_path}${local_dataset_folder} ${hadoop_folder}