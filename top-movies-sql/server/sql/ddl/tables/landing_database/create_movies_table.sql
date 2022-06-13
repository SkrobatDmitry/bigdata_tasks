USE landing_db;
DROP TABLE IF EXISTS lnd_movies;

CREATE TABLE lnd_movies (
    id          INT UNSIGNED    NOT NULL         AUTO_INCREMENT,
    movie_id    VARCHAR(255)    DEFAULT NULL,
    title       VARCHAR(255)    DEFAULT NULL,
    genres      VARCHAR(255)    DEFAULT NULL,

    CONSTRAINT pk_lnd_movies PRIMARY KEY (id)
);