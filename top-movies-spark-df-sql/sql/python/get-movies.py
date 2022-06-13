import argparse
from pyspark import SparkConf
from pyspark.sql import SparkSession


def get_args() -> dict:
    """
    It parses the arguments passed to the script
    :return: A dictionary with the arguments as keys and their values as values
    """
    parser = argparse.ArgumentParser(add_help=False)

    filter_group = parser.add_argument_group()
    filter_group.add_argument('--N', type=int, default=0)
    filter_group.add_argument('--regexp', type=str, default='')
    filter_group.add_argument('--year-from', type=int, default=0)
    filter_group.add_argument('--year-to', type=int, default=10000)
    filter_group.add_argument('--genres', type=str, default='Action|Adventure|Animation|Children|Comedy|Crime|'
                                                            'Documentary|Drama|Fantasy|Film-Noir|Horror|IMAX|Musical|'
                                                            'Mystery|Romance|Sci-Fi|Thriller|War|Western')
    path_group = parser.add_argument_group()
    path_group.add_argument('--path', type=str, required=True)

    return vars(parser.parse_args())


def create_db(spark: SparkSession, path: str) -> None:
    """It creates a movies database"""
    spark.sql(f'DROP DATABASE IF EXISTS movies_db CASCADE')
    spark.sql(f'CREATE DATABASE movies_db COMMENT "This is movies database" LOCATION "{path}/movies_db"')


def create_movies_view(spark: SparkSession, path: str) -> None:
    """It creates a temporary view called that is based on the movies CSV file"""
    spark.sql(f'DROP VIEW IF EXISTS lnd_movies')
    spark.sql(f'''
        CREATE TEMPORARY VIEW lnd_movies
        (
            movie_id STRING,
            title STRING,
            genres STRING
        )
        USING CSV
        OPTIONS
        (
            HEADER = true,
            PATH = '{path}/dataset/movies.csv'
        )
    ''')


def create_ratings_view(spark: SparkSession, path: str) -> None:
    """It creates a temporary view called lnd_ratings that is based on the ratings CSV file"""
    spark.sql(f'DROP VIEW IF EXISTS lnd_ratings')
    spark.sql(f'''
        CREATE TEMPORARY VIEW lnd_ratings
        (
            user_id STRING,
            movie_id STRING,
            rating STRING,
            timestamp STRING
        )
        USING CSV
        OPTIONS
        (
            HEADER = true,
            PATH = '{path}/dataset/ratings.csv'
        )
    ''')


def create_movies_table(spark: SparkSession, path: str) -> None:
    """It creates a table, that has four columns: title, year, genre, and rating"""
    spark.sql(f'DROP TABLE IF EXISTS movies_db.mapped_and_filtered_movies')
    spark.sql(f'''
        CREATE TABLE movies_db.mapped_and_filtered_movies
        (
            title STRING,
            year INT,
            genre STRING,
            rating FLOAT
        )
        LOCATION '{path}/movies_db/mapped_and_filtered_movies'
    ''')


def load_movies_table(spark: SparkSession, args: dict) -> None:
    """It maps and filters movies view and ratings view and writes the result of a SQL query to a Hive table"""
    split_regexp = '(.+)[ ]+[(](\\\d{4})[)]'
    spark.sql(f'''
        WITH cte_split AS
        (
            SELECT
                movie_id,
                REGEXP_EXTRACT(title, '{split_regexp}', 1) AS title,
                REGEXP_EXTRACT(title, '{split_regexp}', 2) AS year,
                EXPLODE(SPLIT(genres, '[|]')) AS genre
            FROM lnd_movies
        ),
        cte_required_genres AS
        (
            SELECT EXPLODE(SPLIT('{args['genres']}', '[|]')) AS genre
        )
        INSERT OVERWRITE TABLE movies_db.mapped_and_filtered_movies
            SELECT title, year, genre, rating
            FROM
            (
                SELECT 
                    title, year, genre, rating,
                    ROW_NUMBER() OVER(PARTITION BY genre ORDER BY rating DESC, year DESC, title) AS num
                FROM cte_split
                JOIN
                (
                    SELECT movie_id, ROUND(AVG(rating), 4) as rating
		            FROM lnd_ratings
		            GROUP BY (movie_id)
                ) USING (movie_id)
                WHERE
                    year != 0 AND year BETWEEN {args['year_from']} AND {args['year_to']}
                    AND LOCATE('{args['regexp']}', title) != 0
                    AND genre IN (SELECT genre FROM cte_required_genres)
            ) AS result
            WHERE num <= {args['N']} OR {args['N']} = 0
            ORDER BY genre, rating DESC, year DESC, title
    ''')


def create_result_table(spark: SparkSession, path: str) -> None:
    """It creates a table using the data from the mapped and filtered movies table and saves it on CSV file"""
    spark.sql(f'DROP TABLE IF EXISTS movies_db.result_movies')
    spark.sql(f'''
        CREATE TABLE movies_db.result_movies
        USING CSV
        OPTIONS
        (
            HEADER = true
        )
        LOCATION '{path}/movies_db/result_movies'
        AS
        (
            SELECT genre, title, year, rating 
            FROM movies_db.mapped_and_filtered_movies
        )
    ''')


def main():
    conf = SparkConf().setAppName('top_movies_spark_sql').set('spark.sql.catalogImplementation', 'hive')
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    args = get_args()

    create_db(spark, args['path'])
    create_movies_view(spark, args['path'])
    create_ratings_view(spark, args['path'])

    create_movies_table(spark, args['path'])
    load_movies_table(spark, args)

    create_result_table(spark, args['path'])


if __name__ == '__main__':
    main()
