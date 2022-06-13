USE landing_db;
DROP TABLE IF EXISTS lnd_ratings;

CREATE TABLE lnd_ratings (
    id             INT UNSIGNED    NOT NULL         AUTO_INCREMENT,
    user_id        VARCHAR(255)    DEFAULT NULL,
    movie_id       VARCHAR(255)    DEFAULT NULL,
    rating         VARCHAR(255)    DEFAULT NULL,
    `timestamp`    VARCHAR(255)    DEFAULT NULL,

    CONSTRAINT pk_lnd_ratings PRIMARY KEY (id)
);