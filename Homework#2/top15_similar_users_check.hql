/************************************************************************	
    HW2 Q3(c): SELECTING THE TOP3 MOST SIMILAR USERS OF EACH USER
    SID: 1155162635
*************************************************************************/


-- Prerequisite: Create if not exists, truncate the table and refill for debugging & reloading
CREATE TABLE if not exists movielens_large(
    user_id INT,
    movie_id INT)
COMMENT 'hw2_movielens_large'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;
TRUNCATE TABLE movielens_large;
LOAD DATA INPATH './movielens_large' OVERWRITE INTO TABLE movielens_large;

-- Same logic as PIG Latin:
-- #1: Create (user1,user2,common_cnt) with INNER JOIN
-- #2: Create (user1,user2,common_cnt,user1_cnt,user2_cnt) with LEFT JOIN
-- #3: Calculate similarity as common_cnt/(user1_cnt + user2_cnt - common_cnt), and rank the instance by similarity with WINDOW FUNCTIONS
WITH sim_rank_table AS(
    SELECT
        uid1,uid2,
        cmn_cnt, 
        user1_cnt+user2_cnt-cmn_cnt AS all_cnt, 
        cmn_cnt/ (user1_cnt+user2_cnt-cmn_cnt) AS sim,
        ROW_NUMBER() over (PARTITION BY uid1 ORDER BY cmn_cnt/ (user1_cnt+user2_cnt-cmn_cnt)  DESC) as sim_rank
    FROM(
        SELECT
            uid1,uid2,COUNT(mid) AS cmn_cnt
        FROM(
            SELECT
            t1.movie_id AS mid, t1.user_id as uid1, t2.user_id AS uid2
            FROM movielens_large t1
            JOIN movielens_large t2 ON t1.movie_id = t2.movie_id AND t1.user_id!=t2.user_id
        ) inner_table
        GROUP BY uid1,uid2
    ) cmn_movie_table
    LEFT JOIN
        (SELECT user_id, COUNT(movie_id) AS user1_cnt FROM movielens_large GROUP BY user_id) t3 ON t3.user_id = cmn_movie_table.uid1
    LEFT JOIN
        (SELECT user_id, COUNT(movie_id) AS user2_cnt FROM movielens_large GROUP BY user_id) t4 ON t4.user_id = cmn_movie_table.uid2
)

-- Testing Proposes
SELECT * FROM  sim_rank_table
WHERE CAST(uid1 AS STRING) LIKE CONCAT("%","2635") AND sim_rank<=15; 