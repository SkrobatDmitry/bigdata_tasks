USE target_db;

INSERT INTO movies (movie_id, title, year, genre, rating)
	WITH RECURSIVE cte_split (movie_id, title, year, genre, remain_genres) AS
    (
		/* Anchor member */
		SELECT
            movie_id,
            SUBSTR(title, 1, LENGTH(title) - 7),
            CAST(SUBSTR(REGEXP_SUBSTR(title, '[(][0-9]{4}[)]$'), 2, 4) AS UNSIGNED INT),
            LEFT(genres, LOCATE('|', CONCAT(genres, '|')) - 1),
            INSERT(CONCAT(genres, '|'), 1, LOCATE('|', CONCAT(genres, '|')), '')
        FROM landing_db.lnd_movies
        UNION ALL

        /* Recursive member */
        SELECT
            movie_id,
            title,
            year,
            LEFT(remain_genres, LOCATE('|', remain_genres) - 1),
            INSERT(remain_genres, 1, LOCATE('|', remain_genres), '')
        FROM cte_split

        /* Terminate condition */
        WHERE LOCATE('|', remain_genres) <> 0
    )
	SELECT movie_id, title, year, genre, rating
	FROM cte_split
	JOIN
    (
		/* Join an average rating for (movie_id) */
		SELECT movie_id, ROUND(AVG(rating), 4) as rating
		FROM landing_db.lnd_ratings
		GROUP BY (movie_id)
    ) AS avg_rating USING (movie_id)

    /* Checking for the correctness of
       movie_id (Is an integer)
       year     (Is the year the movie listed)
       genre    (Is the genre of the film specified)
	*/
    WHERE
        movie_id NOT LIKE '%[^0-9]%'
        AND year != 0
        AND genre != '(no genres listed)'
    ORDER BY CAST(movie_id AS UNSIGNED INT);