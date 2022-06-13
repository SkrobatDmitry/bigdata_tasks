import os
import sys
import argparse

import yaml
from mysql.connector import MySQLConnection


def get_args(config: dict) -> dict:
    """Returns dictionary with parsed arguments from the command line"""
    parser = argparse.ArgumentParser(description='This utility allows user to get information about top rated films')
    group = parser.add_argument_group()

    group.add_argument('--N', type=int, metavar='amount', default=config['n'],
                       help='number of movies of each rating to output')
    group.add_argument('--year-from', type=int, metavar='year', default=config['year_from'],
                       help='the first filter by the year the movie was made')
    group.add_argument('--year-to', type=int, metavar='year', default=config['year_to'],
                       help='the second filter by the year the movie was made')
    group.add_argument('--regexp', type=str, metavar='regexp', default=config['regexp'],
                       help='filter of movies title or their parts')
    group.add_argument('--genres', type=str, metavar='genres', default=config['genres'],
                       help='requested movies genres')

    return vars(parser.parse_args())


def get_config(path: str) -> dict:
    """Reads the config yaml file and returns a dictionary of the config parameters"""
    try:
        path = os.path.abspath(os.path.dirname(__file__)) + '/' + path
        with open(path) as config_file:
            config = yaml.safe_load(config_file)
            return config
    except Exception as e:
        print(e, file=sys.stderr)
        raise SystemExit


def get_movies(args: tuple, database: dict, stored_procedure: dict, batch_size: int, memory_limit: int) -> list:
    """
    :param args: A tuple of arguments for finding the required movies
    :param database: A dictionary containing the database connection information
    :param stored_procedure: The name of the stored procedure to call
    :param batch_size: The number of rows to return at a time
    :param memory_limit: The maximum amount of memory the function is allowed to use in bytes
    :return: A list of tuples of movie data
    """
    try:
        connection = MySQLConnection(**database)
        with connection.cursor() as cursor:
            cursor.callproc(stored_procedure['get_movies'], args=args)

            movies = []
            while True:
                result = []
                if not movies:
                    movies = [res.fetchmany(batch_size) for res in cursor.stored_results()][0]

                while sys.getsizeof(result) < memory_limit and movies:
                    result.extend(movies)
                    movies = [res.fetchmany(batch_size) for res in cursor.stored_results()][0]

                if result:
                    yield result
                else:
                    break
    except Exception as e:
        print(e, file=sys.stderr)
        raise SystemExit


def get_csv_like_movies(args: dict, database_config: dict, config: dict) -> str:
    """
    Given a list of arguments, it returns a string of CSV-like movies
    :return: A string of CSV-like data
    """
    result, delimiter = config['csv']['head'], config['csv']['delimiter']

    for movies in get_movies(args=tuple(args.values()), **database_config, **config['memory']):
        for movie in movies:
            result += '\n' + (delimiter.join(map(str, movie)))
        yield result


def show_movies(movies: str) -> None:
    """Prints the movies to the console"""
    print(movies)


def main():
    config, database_config = get_config('config/config.yaml'), get_config('config/database_config.yaml')
    args = get_args(config['default'])

    for movies in get_csv_like_movies(args, database_config, config):
        show_movies(movies)


if __name__ == "__main__":
    main()
