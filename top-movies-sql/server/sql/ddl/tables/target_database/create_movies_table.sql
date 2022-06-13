USE target_db;
DROP TABLE IF EXISTS movies;

CREATE TABLE movies (
    id          INT             UNSIGNED    NOT NULL     AUTO_INCREMENT,
    movie_id    INT             UNSIGNED    NOT NULL,
    title       VARCHAR(255)                NOT NULL,
    year        SMALLINT        UNSIGNED    NOT NULL,
    genre       VARCHAR(255)                NOT NULL,
    rating      FLOAT(5, 4)     UNSIGNED    NOT NULL,

    CONSTRAINT pk_lnd_ratings PRIMARY KEY (id)
);