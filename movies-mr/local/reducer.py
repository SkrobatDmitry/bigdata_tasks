#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import re
import argparse
from operator import itemgetter
from itertools import groupby


def shuffle() -> tuple:
    """
    It takes a list of movies and groups them by genres
    :return: A tuple of the movie genre and a list of tuples of the movie title and year
    """
    with sys.stdin as data:
        movies = [line.strip().split('\t') for line in data]
        movie_groups = {key: [get_movie_tuple(value) for _, value in group if get_movie_tuple(value)]
                        for key, group in groupby(movies, key=itemgetter(0))}

    for key, values in movie_groups.items():
        yield key, values


def get_movie_tuple(movie: str) -> tuple:
    """
    It splits the movie string on commas, but ignores commas that are inside quotes

    :param movie: The movie string to parse
    :return: A tuple of the movie title and the year it was released
    """
    try:
        title, year = re.split(',(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)', movie[1:-1])
        return title[1:-1], int(year[1:])
    except Exception as e:
        print(e, file=sys.stderr)
        return None, None


def reduce(key: str, values: list, number: int = None) -> tuple:
    """
    It returns the key and the N values sorted by the movie year in descending order
    and the movie title in ascending order

    :param key: The movie genre
    :param values: A list of tuples of the movie title and year
    :param number: The number of movies to return
    :return: A tuple of the form (key, value)
    """
    values.sort(key=lambda x: (-x[1], x[0]))
    return (key, values) if not number else (key, values[:number])


def get_args() -> dict:
    """
    It parses the arguments passed to the script
    :return: A dictionary with the arguments as keys and their values as values
    """
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--N', type=int)

    return vars(parser.parse_args())


def main():
    args = get_args()

    for genre, movies in shuffle():
        key, values = reduce(genre, movies, args['N'])
        for title, year in values:
            title = title.center(len(title) + 2, '"') if ',' in title else title
            print(f'{key},{title},{year}', file=sys.stdout)


if __name__ == '__main__':
    main()
