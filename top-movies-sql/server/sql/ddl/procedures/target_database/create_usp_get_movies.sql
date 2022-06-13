USE target_db;
DROP PROCEDURE IF EXISTS usp_get_movies;

DELIMITER //
CREATE PROCEDURE usp_get_movies (
	IN n         SMALLINT UNSIGNED,
    IN year_from SMALLINT UNSIGNED,
    IN year_to   SMALLINT UNSIGNED,
    IN regex     VARCHAR(255),
    IN genres    VARCHAR(255)
)
BEGIN
	WITH RECURSIVE cte_split (required_genre, remain_genres) AS
    (
		/* Anchor member */
		SELECT
            LEFT(genres, LOCATE('|', CONCAT(genres, '|')) - 1),
            INSERT(CONCAT(genres, '|'), 1, LOCATE('|', CONCAT(genres, '|')), '')
        UNION ALL

        /* Recursive member */
        SELECT
            LEFT(remain_genres, LOCATE('|', remain_genres) - 1),
            INSERT(remain_genres, 1, LOCATE('|', remain_genres), '')
        FROM cte_split

        /* Terminate condition */
        WHERE LOCATE('|', remain_genres) <> 0
    )
	SELECT genre, escape_title(title), year, rating FROM
    (
		SELECT
			title, year, genre, rating, required_genre,
		    /* Serial number of the movie that meets the criteria */
            ROW_NUMBER() OVER(PARTITION BY genre ORDER BY rating DESC, year DESC, title) AS num
		FROM movies
        JOIN
        (
			SELECT required_genre
            FROM cte_split
        ) AS required_genre

	    /* Criteria check */
        WHERE
			year BETWEEN year_from AND year_to
			AND LOCATE(regex, title) != 0
            AND genre = required_genre
    ) AS result

	/* Checking to output only N required number of movies */
    WHERE num <= n OR n = 0
    ORDER BY genre, rating DESC, year DESC, title;
END //
DELIMITER ;