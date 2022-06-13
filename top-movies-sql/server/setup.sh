#!/bin/bash
source utils/load_dataset.sh
source utils/parse_yaml.sh

# Default value for cmd arguments
is_load=false

function setup_landing_database() {
  # Get database parameters
  eval $(parse_yaml $1 landing_)

  # Create landing database
  mysql --user=$landing_database_user --password=$landing_database_password \
        --host=$landing_database_host --port=$landing_database_port \
        < "$landing_sql_databases/create_landing_database.sql"

  # Create and load movies table for landing database
  mysql --user=$landing_database_user --password=$landing_database_password \
        --host=$landing_database_host --port=$landing_database_port \
        < "$landing_sql_tables/create_movies_table.sql"
  mysql --user=$landing_database_user --password=$landing_database_password \
        --host=$landing_database_host --port=$landing_database_port \
        < "$landing_sql_tables/create_ratings_table.sql"

  # Create and load ratings table for landing database
  mysql --user=$landing_database_user --password=$landing_database_password \
        --host=$landing_database_host --port=$landing_database_port \
        < "$landing_sql_population_scripts/load_movies.sql"
  mysql --user=$landing_database_user --password=$landing_database_password \
        --host=$landing_database_host --port=$landing_database_port \
        < "$landing_sql_population_scripts/load_ratings.sql"
}

function setup_target_database() {
  # Get database parameters
  eval $(parse_yaml $1 target_)

  # Create target database
  mysql --user=$target_database_user --password=$target_database_password \
        --host=$target_database_host --port=$target_database_port \
        < "$target_sql_databases/create_target_database.sql"

  # Create and load movies table for target database
  mysql --user=$target_database_user --password=$target_database_password \
        --host=$target_database_host --port=$target_database_port \
        < "$target_sql_tables/create_movies_table.sql"
  mysql --user=$target_database_user --password=$target_database_password \
        --host=$target_database_host --port=$target_database_port \
        < "$target_sql_population_scripts/load_movies.sql"

  # Create function for escaping title and storage procedure for getting movies
  mysql --user=$target_database_user --password=$target_database_password \
        --host=$target_database_host --port=$target_database_port \
        < "$target_sql_functions/create_escape_title.sql"
  mysql --user=$target_database_user --password=$target_database_password \
        --host=$target_database_host --port=$target_database_port \
        < "$target_sql_procedures/create_usp_get_movies.sql"
}

function print_help() {
  echo "usage: setup.sh [-h] [-l]

The script is used to set up the database

optional arguments:
    -h, --help   show this help message and exit
    -l, --load   determines the need to download the dataset"
}

# Parse command prompt arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    -l|--load)
      is_load=true
      shift
      shift
      ;;
    -h|--help)
      print_help
      exit 0
      ;;
    *)
      print_help
      exit 1
  esac
done

# Get config arguments
eval $(parse_yaml config/config.yaml)

if [ $is_load == true ] ; then
  load_dataset $dataset
  echo "Dataset $dataset: downloaded successfully"
fi

# Setup databases
setup_landing_database $database_config_landing
setup_target_database $database_config_target
echo "Databases: setup successfully"
