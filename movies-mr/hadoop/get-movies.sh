#!/bin/bash

# Argument parameters initialization
mapper_args=""
reducer_args=""
full_path=""

function print_help() {
  echo "usage: $0 [-h] [--N amount] [--genres genres] [--year-from year] [--year-to year] [--regexp regexp]

optional arguments:
    -h, --help  show this help message and exit

    --N amount        number of movies of each genre to output
    --genres genres   requested movies genres
    --year-from year  the first filter by the year the movie was made
    --year-to year    the second filter by the year the movie was made
    --regexp regexp   filter of movies title or their parts"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
  -h | --help)
    print_help
    exit 0
    ;;
  --N)
    reducer_args+="$1 $2 "
    shift 2
    ;;
  --genres | --year-from | --year-to | --regexp)
    mapper_args+="$1 $2 "
    shift 2
    ;;
  *)
    print_help 1>&2
    exit 1
    ;;
  esac
done

# Get full path to the folder of executable script
if [[ ${BASH_SOURCE[0]} == *"/"* ]]; then
  full_path="$(cd "$(echo "${BASH_SOURCE[0]%/*}")"; pwd)/"
fi

# Get config arguments
source "${full_path}utils/parse_yaml.sh"
eval $(parse_yaml "${full_path}config/config.yaml")

# Get full path to mapper, reducer and hadoop streaming
mapper_path="${full_path}mapper.py"
reducer_path="${full_path}reducer.py"
hadoop_streaming_path="$(sudo find / -name hadoop-streaming.jar)"

# Remove hdfs output directory if it exists
hdfs dfs -test -d ${hadoop_output_folder}
if [ $? -eq 0 ]; then
  hdfs dfs -rm -r ${hadoop_output_folder} &>/dev/null
fi

yarn jar ${hadoop_streaming_path} \
    -files ${mapper_path},${reducer_path} \
    -input "${hadoop_dataset_folder}/movies.csv" \
    -output ${hadoop_output_folder} \
    -mapper "$PYSPARK_PYTHON mapper.py ${mapper_args}" \
    -reducer "$PYSPARK_PYTHON reducer.py ${reducer_args}"