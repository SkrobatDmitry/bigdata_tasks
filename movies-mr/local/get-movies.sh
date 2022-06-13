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

# Get full path to mapper, reducer and dataset
mapper_path="${full_path}mapper.py"
reducer_path="${full_path}reducer.py"
dataset_path="${full_path}${dataset_folder}/movies.csv"

cat ${dataset_path} | python3 ${mapper_path} ${mapper_args} | sort | python3 ${reducer_path} ${reducer_args}
