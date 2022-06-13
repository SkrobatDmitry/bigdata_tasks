#!/usr/bin/env python3

import sys
import re
import argparse
from itertools import islice
from pyspark import SparkContext, SparkConf, rdd


def get_args() -> dict:
    """
    It parses the arguments passed to the script
    :return: A dictionary with the arguments as keys and their values as values
    """
    parser = argparse.ArgumentParser(add_help=False)

    filter_group = parser.add_argument_group()
    filter_group.add_argument('--N', type=int)
    filter_group.add_argument('--regexp', type=str, default='')
    filter_group.add_argument('--year-from', type=int, default=0)
    filter_group.add_argument('--year-to', type=int, default=10000)
    filter_group.add_argument('--genres', type=str)

    path_group = parser.add_argument_group()
    path_group.add_argument('--dataset', type=str, required=True)
    path_group.add_argument('--output', type=str, required=True)

    return vars(parser.parse_args())


def get_rdd_from_csv(sc: SparkContext, path: str, is_header: bool = True) -> rdd:
    """
    It reads a CSV file from FS and returns is as an RDD of strings excluding header
    :return: An RDD object
    """
    try:
        lnd_rdd = sc.textFile(path)
        return lnd_rdd.mapPartitionsWithIndex(lambda i, it: islice(it, 1, None) if i == 0 else it) if is_header else lnd_rdd
    except Exception as e:
        print(e, file=sys.stderr)
        raise SystemExit


def get_movies_rdd(sc: SparkContext, args: dict) -> rdd:
    """
    It reads a movies CSV file, tokenizes each line, and then filters them out
    :return: An RDD of tuples where each tuple is a movie id and a tuple of its title, year and genre
    """

    def get_movie_tuples(movie_line: str) -> list:
        """
        It splits a movie strings on commas, but ignores commas that are inside quotes
        :return: A list of tuples where each tuple is a movie id and a tuple of its title, year and genre
        """
        try:
            movie_id, title, genres = re.split(',(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)', movie_line)
            title = title[1:-1] if title.startswith('"') and title.endswith('"') else title

            movie_title, movie_year = re.findall('(.*)[ ]\((\d{4})\)$', title)[0]

            return [(int(movie_id), (movie_title, int(movie_year), movie_genre)) for movie_genre in genres.split('|')]
        except Exception as e:
            print(e, file=sys.stderr)
            return []

    def match_movie(movie_tuple: tuple) -> bool:
        """It returns a boolean indicating whether the movie matches the genre, title and range of years criteria"""

        def match_genre(genre: str, genres: str) -> bool:
            """It returns a boolean indicating whether the movie genre is in the list of genres"""
            return genre.lower() in genres.lower() if genres else genre != '(no genres listed)'

        def match_title(title: str, regexp: str) -> bool:
            """It returns a boolean indicating whether the movie title matches the regular expression"""
            return bool(re.search(regexp, title, re.IGNORECASE))

        def match_year(year: int, year_from: int, year_to: int) -> bool:
            """It returns a boolean indicating whether the movie year is in a range of years"""
            return year_from <= year <= year_to

        movie_id, movie = movie_tuple
        movie_title, movie_year, movie_genre = movie

        return match_genre(movie_genre, args['genres']) and match_title(movie_title, args['regexp']) \
            and match_year(movie_year, args['year_from'], args['year_to'])

    raw_movies_rdd = get_rdd_from_csv(sc, f'{args["dataset"]}/movies.csv')
    preprocessed_movies_rdd = raw_movies_rdd.flatMap(get_movie_tuples)
    return preprocessed_movies_rdd.filter(match_movie)


def get_ratings_rdd(sc: SparkContext, args: dict) -> rdd:
    """
    It reads a ratings CSV file, tokenizes each line, and calculates the average rating for each movie
    :return: An RDD of tuples where each tuple is a movie id and the average rating of the movie
    """

    def get_rating_tuple(rating_line: str) -> tuple:
        """
        It takes a line from the ratings file and split it by comma
        :return: A tuple of the movie id and a list containing the rating and the number of ratings
        """
        try:
            _, movie_id, rating, _ = rating_line.split(',')
            return int(movie_id), [float(rating), 1]
        except Exception as e:
            print(e, file=sys.stderr)
            return ()

    def combine_rating(first_rating_tuple: tuple, second_rating_tuple: tuple) -> tuple:
        """
        It takes two tuples and sums their ratings and amounts
        :return: A tuple of the combined rating and the combined amount
        """
        first_rating, first_amount = first_rating_tuple
        second_rating, second_amount = second_rating_tuple
        return first_rating + second_rating, first_amount + second_amount

    def get_average_rating(rating_tuple: tuple) -> float:
        """
        It divides the rating by the amount, and rounds the result to 4 decimal places
        :return: The average rating of the movie
        """
        rating, amount = rating_tuple
        return round(rating / amount, 4)

    raw_ratings_rdd = get_rdd_from_csv(sc, f'{args["dataset"]}/ratings.csv')
    preprocessed_ratings_rdd = raw_ratings_rdd.map(get_rating_tuple)
    return preprocessed_ratings_rdd.reduceByKey(combine_rating).mapValues(get_average_rating)


def get_movies_ratings_rdd(movies_rdd: rdd, ratings_rdd: rdd, args: dict) -> rdd:
    """
    It joins the movies and ratings RDDs, reorganizes the tuples, groups the movies by genre,
    and finally, it returns the N movies with the highest average rating for each genre
    :return: An RDD of tuples where each tuples is a movie genre and a tuple of its title, year and rating
    """

    def reorganize_tuple(joint_tuple: tuple) -> tuple:
        """
        It takes a tuple of the form (movie_id, ((movie_title, movie_year, movie_genre), movie_rating))
        :return: A tuple of the form (movie_genre, (movie_title, movie_year, movie_rating))
        """
        movie_id, movie_rating_tuple = joint_tuple

        movie_tuple, movie_rating = movie_rating_tuple
        movie_title, movie_year, movie_genre = movie_tuple

        return movie_genre, (movie_title, movie_year, movie_rating)

    def get_top_n_values(movie_rating_tuples: list) -> list:
        """
        It sorts the list of tuples by the rating, then by the movie year, then by the movie title
        :return: The N movies with the highest average rating
        """
        movie_rating_tuples.sort(key=lambda x: (-x[2], -x[1], x[0]))
        return movie_rating_tuples[:args['N']] if args['N'] else movie_rating_tuples

    preprocessed_movies_ratings_rdd = movies_rdd.join(ratings_rdd).map(reorganize_tuple)
    grouped_movies_ratings_rdd = preprocessed_movies_ratings_rdd.groupByKey().sortByKey()
    return grouped_movies_ratings_rdd.mapValues(lambda v: get_top_n_values(list(v)))


def get_csv_list(key_value_pair: tuple, separator: str = ',') -> list:
    """
    It takes a key-value pair, where the key is the movie genre and the value is a list of movies
    :return: A list of CSV-like movie strings
    """

    def get_escaped_value(value: str, escape_char: str = '"') -> str:
        """It returns the value is being returned with the escape character on either side of the value if needed"""
        return value.center(len(value) + 2, escape_char) if separator in value else value

    genre, movies = key_value_pair
    return [f'{genre}{separator}{get_escaped_value(title)}{separator}{year}{separator}{rating}'
            for title, year, rating in movies]


def get_csv_rdd(sc: SparkContext, movies_ratings_rdd: rdd, header: str) -> rdd:
    """It takes an RDD of movies and ratings and returns an RDD of CSV strings with header"""
    csv_rdd = movies_ratings_rdd.flatMap(get_csv_list)
    return sc.parallelize([header]).union(csv_rdd) if header else csv_rdd


def main():
    conf = SparkConf().setAppName('top_movies_spark_core').setMaster('local[*]')
    sc = SparkContext(conf=conf)

    args = get_args()

    movies_rdd = get_movies_rdd(sc, args)
    ratings_rdd = get_ratings_rdd(sc, args)

    movies_ratings_rdd = get_movies_ratings_rdd(movies_rdd, ratings_rdd, args)

    csv_rdd = get_csv_rdd(sc, movies_ratings_rdd, 'genre,title,year,rating')
    csv_rdd.saveAsTextFile(args['output'])


if __name__ == '__main__':
    main()
