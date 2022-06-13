USE landing_db;

LOAD DATA INFILE 'dataset/movies.csv'
INTO TABLE lnd_movies
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(movie_id, title, genres);