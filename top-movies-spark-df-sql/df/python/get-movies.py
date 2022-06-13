#!/usr/bin/env python3
import sys
import re
import argparse
from pyspark.sql import SparkSession, Window, dataframe
from pyspark.sql.functions import udf, explode, split, lit, round, mean, row_number
from pyspark.sql.types import IntegerType, StringType, BooleanType


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


def get_csv_df(spark: SparkSession, path: str, header: bool = True) -> dataframe:
    """
    It reads a CSV file from the specified path
    :return: A dataframe
    """
    try:
        return spark.read.csv(path, header=header, inferSchema=True)
    except Exception as e:
        print(e, file=sys.stderr)
        raise SystemExit


@udf(StringType())
def get_title_from_title_udf(title: str):
    """
    It takes a string, removes the year from the end of the string
    :return: The movie title
    """
    movie_title = re.sub(r'[ ]\((\d{4})\)$', '', title)
    return movie_title


@udf(IntegerType())
def get_year_from_title_udf(title: str):
    """
    It takes a string as input, and returns the year in the string if it exists
    :return: The year of the movie
    """
    try:
        movie_year = re.findall(r'[ ]\((\d{4})\)$', title)[0]
        return int(movie_year)
    except Exception as e:
        print(e, file=sys.stderr)
        return None


@udf(BooleanType())
def match_movie_udf(m_title: str, m_year: int, m_genre: str, regexp: str, year_from: int, year_to: int, genres: str):
    """It returns a boolean indicating whether the movie matches the genre, title and range of years criteria"""

    def match_title(title: str):
        """It returns a boolean indicating whether the movie title matches the regular expression"""
        try:
            return bool(re.search(regexp, title, re.IGNORECASE))
        except Exception as e:
            print(e, file=sys.stderr)
            return False

    def match_year(year: int):
        """It returns a boolean indicating whether the movie year is in a range of years"""
        try:
            return year_from <= year <= year_to
        except Exception as e:
            print(e, file=sys.stderr)
            return False

    def match_genre(genre: str):
        """It returns a boolean indicating whether the movie genre is in the list of genres"""
        try:
            return genre.lower() in genres.lower() if genres else genre != '(no genres listed)'
        except Exception as e:
            print(e, file=sys.stderr)
            return False

    return match_genre(m_genre) and match_title(m_title) and match_year(m_year)


def get_movies_df(spark: SparkSession, args: dict) -> dataframe:
    """
    It reads the movies CSV file, extracts the movie id, title, year and genre from each row,
    and then filters the dataframe based on the arguments passed in
    :return: A dataframe
    """
    lit_movie_args = [lit(v) for k, v in args.items() if k in ['regexp', 'year_from', 'year_to', 'genres']]

    df = get_csv_df(spark, f"{args['dataset']}/movies.csv")
    df = df.select(df['movieId'].alias('movie_id'), get_title_from_title_udf('title').alias('title'),
                   get_year_from_title_udf('title').alias('year'), explode(split('genres', r'\|')).alias('genre'))

    return df.filter(match_movie_udf('title', 'year', 'genre', *lit_movie_args))


def get_ratings_df(spark: SparkSession, args: dict) -> dataframe:
    """
    It reads the ratings CSV file, select the movie id and rating columns, filter out null ratings,
    group by movie_id, and calculate the average rating for each movie
    :return: A dataframe with the average rating for each movie
    """
    df = get_csv_df(spark, f"{args['dataset']}/ratings.csv")
    df = df.select(df['movieId'].alias('movie_id'), df['rating']).filter(df['rating'].isNotNull())
    return df.groupBy('movie_id').agg(round(mean('rating'), 4).alias('rating'))


def get_movies_ratings_df(movies_df: dataframe, ratings_df: dataframe, args: dict) -> dataframe:
    """
    It joins the movies and ratings dataframes, adds a row number column, filters the dataframe
    if the user specified a value for N, and then sorts the dataframe by genre, rating, title, and year
    :return: A dataframe
    """
    df = movies_df.join(ratings_df, movies_df['movie_id'] == ratings_df['movie_id'], 'inner')
    df = df.select(movies_df['genre'], movies_df['title'], movies_df['year'], ratings_df['rating']) \
        .withColumn('row_number', row_number().over(Window.partitionBy('genre').orderBy(ratings_df['rating'].desc(),
                                                                                        movies_df['year'].desc(),
                                                                                        movies_df['title'].asc())))
    if args['N']:
        df = df.filter(df.row_number <= args['N'])

    return df.sort(df['genre'], df['row_number']).select(df['genre'], df['title'], df['year'], df['rating'])


def main():
    spark = SparkSession.builder.appName('top_movies_spark_df').getOrCreate()
    args = get_args()

    movies_df = get_movies_df(spark, args)
    ratings_df = get_ratings_df(spark, args)

    movies_ratings_df = get_movies_ratings_df(movies_df, ratings_df, args)
    movies_ratings_df.write.csv(args['output'], encoding='utf-8', header=True)


if __name__ == '__main__':
    main()
