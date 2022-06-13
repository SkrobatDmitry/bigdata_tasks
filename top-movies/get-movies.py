import os
import re
import sys
import argparse
import configparser


class TopMovies:
    def __init__(self, args: dict, path: dict):
        self.__args = args
        self.__path = {'movies': os.path.abspath(os.path.dirname(__file__)) + '/' + path['movies'],
                       'ratings': os.path.abspath(os.path.dirname(__file__)) + '/' + path['ratings']}

        self.__movies = self.__get_movies()
        self.__ratings = self.__get_ratings()

    def __get_movies(self, batch_size: int = 1e6) -> dict:
        """
        Reads the movies.csv file and returns a dictionary of movie id's and their corresponding title and year
        :param batch_size: The number of bytes to read from the file at a time
        :return: A dictionary of movie_id: (title, year) pairs
        """
        try:
            with open(self.__path['movies'], 'r', encoding='utf-8') as file:
                # Skip header
                file.readline()

                movies = []
                while True:
                    movies_str = file.read(int(batch_size)) + file.readline()
                    if not movies_str:
                        break

                    movies.extend(self.__get_movie_tuple(movie) for movie in re.split('\n', movies_str[:-1])
                                  if self.__match_movie(movie))

                # Sorts movies by year DESC then by title ASC
                movies.sort(key=lambda x: (-x[1][1], x[1][0]))
                return dict(movies)
        except Exception as e:
            print(e, file=sys.stderr)
            raise SystemExit

    def __get_ratings(self, batch_size: int = 1e6):
        """
        Reads the ratings.csv file and returns a dictionary of movie id's and their average rating
        :param batch_size: The number of lines to read in from the ratings file at a time
        :return: A dictionary of movie_id: average_rating pairs
        """
        try:
            with open(self.__path['ratings'], 'r', encoding='utf-8') as file:
                # Skip header
                file.readline()

                ratings = {movie_id: [0, 0.] for movie_id in self.__movies.keys()}
                while True:
                    ratings_str = file.read(int(batch_size)) + file.readline()
                    if not ratings_str:
                        break

                    for rating in re.split('\n', ratings_str[:-1]):
                        _, movie_id, movie_rating, _ = re.split(',', rating)
                        movie_id, movie_rating = int(movie_id), float(movie_rating)

                        if ratings.get(movie_id, False):
                            amount, sum_rating = ratings[movie_id]
                            ratings[movie_id] = [amount + 1, sum_rating + movie_rating]

            ratings = [(movie_id, round(sum_rating / amount, 4))
                       for movie_id, (amount, sum_rating) in ratings.items() if amount]
            ratings.sort(key=lambda x: x[1], reverse=True)
            return dict(ratings)
        except Exception as e:
            print(e, file=sys.stderr)
            raise SystemExit

    def get_top_movies(self):
        """
        Get the top N movies for each genre
        :return: A dictionary with the genres as keys and a list of tuples as values. Each tuple contains
        the title, year, and rating of a movie
        """
        n = self.__args['N'] if self.__args['N'] else len(self.__movies)
        top_movies = {genre: [] for genre in self.__args['genres']}

        for movie_id in self.__ratings.keys():
            for genre in self.__args['genres']:
                if len(top_movies[genre]) < n:
                    movie_title, movie_year, movie_genres = self.__movies[movie_id]
                    if genre in movie_genres:
                        top_movies[genre].append((movie_title, movie_year, self.__ratings[movie_id]))

        return top_movies

    def __match_movie(self, movie: str) -> bool:
        """
        If the movie title matches the regular expression, and the year is within the range, and the genre
        is in the list of genres, then the movie is a match
        :param movie: The movie to match
        :return: A boolean value
        """
        movie_id, title, movie_genres = self.__split_movie(movie)

        if not self.__split_title(title):
            return False

        movie_title, movie_year = self.__split_title(title)[0]
        if self.__args['regexp'] not in movie_title:
            return False
        if not self.__args['year_from'] <= int(movie_year) <= self.__args['year_to']:
            return False

        return any(genre in movie_genres for genre in self.__args['genres'])

    def __get_movie_tuple(self, movie: str) -> tuple:
        """
        Given a movie string, return a tuple of movie id, movie title, and movie genres
        :param movie: The movie string to search for
        :return: A tuple of the movie id, the movie title, the movie year, and the movie genres
        """
        movie_id, title, movie_genres = self.__split_movie(movie)
        movie_title, movie_year = self.__split_title(title)[0]
        return int(movie_id), (movie_title, int(movie_year), movie_genres)

    @staticmethod
    def __split_movie(movie: str) -> list:
        """Split a string into a list of strings using a regex"""
        return re.split(',(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)', movie)

    @staticmethod
    def __split_title(title: str) -> list:
        """Split the title into title and year"""
        return re.findall('(.*)[ ]\((\d{4})\)$', title)


def get_args(config: dict, separator='|') -> dict:
    """
    Parse arguments from the command line
    :return: A dictionary of the arguments passed to the parser
    """
    def get_args_dict(args: argparse.Namespace) -> dict:
        """It takes the arguments from the command line and converts them into a dictionary"""
        args = vars(args)
        args['genres'] = sorted(args['genres'].split(separator))
        return args

    parser = argparse.ArgumentParser(description='This utility allows user to get information about top rated films')
    group = parser.add_argument_group()

    group.add_argument('--N', type=int, metavar='amount',
                       help='the number of movies with the highest rating')
    group.add_argument('--genres', type=str, metavar='genres', default=config['default']['genres'],
                       help='filter by genres - "genre|genre"')
    group.add_argument('--year-from', type=int, metavar='year', default=config['default']['year_from'],
                       help='the first filter by the year the movie was made')
    group.add_argument('--year-to', type=int, metavar='year', default=config['default']['year_to'],
                       help='the second filter by the year the movie was made')
    group.add_argument('--regexp', type=str, metavar='regexp', default=config['default']['regexp'],
                       help='filter (regular expression) by the movie title')

    return get_args_dict(parser.parse_args())


def get_config(config_path: str = 'config.yaml') -> dict:
    """
    Reads the config file and returns a dictionary of the config parameters
    :param config_path: The path to the config file, defaults to config.yaml
    :return: A dictionary of the configuration settings.
    """
    try:
        config = configparser.ConfigParser()
        config.read(os.path.abspath(os.path.dirname(__file__)) + '/' + config_path)
        return config
    except Exception as e:
        print(e, file=sys.stderr)
        raise SystemExit


def show_top_movies(top_movies: dict) -> None:
    """
    Prints the top movies in CSV format
    :param top_movies: A dictionary of genre to a list of tuples of movie title, year, and rating
    """
    top_movies_csv = 'genre,title,year,rating'
    for genre, movies in top_movies.items():
        for movie_title, movie_year, movie_rating in movies:
            movie_title = movie_title.center(len(movie_title) + 2, '"') if ',' in movie_title else movie_title
            top_movies_csv += f'\n{genre},{movie_title},{movie_year},{movie_rating}'
    print(top_movies_csv)


def main():
    config = get_config()
    args = get_args(config)

    top_movies = TopMovies(args, config['path'])
    show_top_movies(top_movies.get_top_movies())


if __name__ == '__main__':
    main()
