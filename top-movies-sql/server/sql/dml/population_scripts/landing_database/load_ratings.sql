USE landing_db;

LOAD DATA INFILE 'dataset/ratings.csv'
INTO TABLE lnd_ratings
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(user_id, movie_id, rating, `timestamp`);