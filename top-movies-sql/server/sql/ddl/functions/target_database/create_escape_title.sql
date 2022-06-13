USE target_db;
DROP FUNCTION IF EXISTS escape_title;

DELIMITER //
CREATE FUNCTION escape_title(title VARCHAR(255)) RETURNS VARCHAR(255)
DETERMINISTIC
BEGIN
    IF LOCATE(',', title) <> 0 THEN
        RETURN CONCAT('"', CONCAT(title, '"'));
    ELSE
        RETURN title;
    END IF;
END; //
DELIMITER ;