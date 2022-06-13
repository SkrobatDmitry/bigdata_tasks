#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import re
import argparse


def do_map(key: int, value: str) -> tuple:
    """
    For each movie, emit a key-value pair for each genre, where the key is the genre and the value is
    the movie title and year

    :param key: The line number read
    :param value: The movie line
    :return: A tuple of the form (key, value)
    """
    title, year, genres = get_movie_tuple(value.strip())
    if title and year and genres:
        for genre in genres.split('|'):
            yield genre, (title, year)


def get_movie_tuple(movie: str) -> tuple:
    """
    It splits the movie string on commas, but ignores commas that are inside quotes

    :param movie: The movie string to parse
    :return: A tuple of the movie title, the movie year, and the movie genres
    """
    try:
        _, title, movie_genres = re.split(',(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)', movie)
        if title.startswith('"') and title.endswith('"'):
            title = title[1:-1]

        find_result = re.findall('(.*)[ ]\((\d{4})\)$', title)
        movie_title, movie_year = find_result[0] if find_result else (title, 0)

        return movie_title, int(movie_year), movie_genres
    except Exception as e:
        print(e, file=sys.stderr)
        return None, None, None


def match_movie(key: str, value: tuple, args: dict) -> bool:
    """
    It returns True if the movie's genre matches the given genre,
    the movie's title matches the given regexp, and the movie's year is within the given range

    :param key: The movie genre
    :param value: A tuple of the movie title and the movie year
    :param args: A set of genres, a title regular expression, and a range of years
    :return: A boolean value
    """

    def match_genre(movie_genre: str, genres: str) -> bool:
        """Given a movie genre and a list of genres, return True if the movie genre is in the list of genres"""
        return movie_genre.lower() in genres.lower() if genres else movie_genre != '(no genres listed)'

    def match_title(movie_title: str, regexp: str) -> bool:
        """Given a movie title and a regexp, return True if the movie title matches the regexp"""
        return re.search(regexp, movie_title, re.IGNORECASE) if regexp else True

    def match_year(movie_year: int, year_from: int, year_to: int) -> bool:
        """Given a movie year and a year range, return True if the movie year in a range of years"""
        is_match = True if movie_year else False

        if movie_year:
            if year_from and year_to:
                is_match = year_from <= movie_year <= year_to
            elif year_from:
                is_match = year_from <= movie_year
            elif year_to:
                is_match = movie_year <= year_to

        return is_match

    genre, (title, year) = key, value
    return match_genre(genre, args['genres']) and match_title(title, args['regexp']) \
           and match_year(year, args['year_from'], args['year_to'])


def get_args() -> dict:
    """
    It parses the arguments passed to the script
    :return: A dictionary with the arguments as keys and their values as values
    """
    parser = argparse.ArgumentParser(add_help=False)
    group = parser.add_argument_group()

    group.add_argument('--regexp', type=str)
    group.add_argument('--genres', type=str)
    group.add_argument('--year-from', type=int)
    group.add_argument('--year-to', type=int)

    return vars(parser.parse_args())


def main():
    args = get_args()

    with sys.stdin as data:
        data.readline()  # Skip header

        for number, line in enumerate(data):
            for key, value in do_map(number, line):
                if match_movie(key, value, args):
                    title, year = value
                    print(f'{key}\t("{title}", {year})', file=sys.stdout)


if __name__ == '__main__':
    main()
