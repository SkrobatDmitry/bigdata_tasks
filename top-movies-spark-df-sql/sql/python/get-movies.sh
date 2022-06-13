#!/bin/bash
source "utils/parse_yaml.sh"

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

args=""
while [[ $# -gt 0 ]]; do
  case $1 in
  -h | --help)
    print_help
    exit 0
    ;;
  --N | --genres | --year-from | --year-to | --regexp)
    args+="$1 $2 "
    shift 2
    ;;
  *)
    print_help 1>&2
    exit 1
    ;;
  esac
done

# Get config argument
eval $(parse_yaml "config/config.yaml")

spark-submit get-movies.py --path ${hadoop_path} ${args} 2>&0
hdfs dfs -cat "${hdfs_output_path}/movies_db/result_movies"