CREATE TABLE if not exists movielens_test(
    user_id INT,
    movie_id INT)
COMMENT 'hw2_movielens_test'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

TRUNCATE TABLE movielens_test;
LOAD DATA INPATH './movielens_test' OVERWRITE INTO TABLE movielens_test;

WITH sim_rank_table AS(
    SELECT
        uid1,uid2,
        -- cmn_cnt, 
        -- user1_cnt+user2_cnt-cmn_cnt AS all_cnt, 
        -- cmn_cnt/ (user1_cnt+user2_cnt-cmn_cnt) AS sim,
        ROW_NUMBER() over (PARTITION BY uid1 ORDER BY cmn_cnt/ (user1_cnt+user2_cnt-cmn_cnt)  DESC) as sim_rank
    FROM(
        SELECT
            uid1,uid2,COUNT(mid) AS cmn_cnt
        FROM(
            SELECT
            t1.movie_id AS mid, t1.user_id as uid1, t2.user_id AS uid2
            FROM movielens_test t1
            JOIN movielens_test t2 ON t1.movie_id = t2.movie_id AND t1.user_id!=t2.user_id
        ) inner_table
        GROUP BY uid1,uid2
    ) cmn_movie_table
    LEFT JOIN
        (SELECT user_id, COUNT(movie_id) AS user1_cnt FROM movielens_test GROUP BY user_id) t3 ON t3.user_id = cmn_movie_table.uid1
    LEFT JOIN
        (SELECT user_id, COUNT(movie_id) AS user2_cnt FROM movielens_test GROUP BY user_id) t4 ON t4.user_id = cmn_movie_table.uid2
)
SELECT uid1,COLLECT_LIST(uid2),COLLECT_LIST(sim_rank)
FROM sim_rank_table
WHERE CAST(uid1 AS STRING) LIKE CONCAT("%","1") AND sim_rank<=3
GROUP BY uid1;

SET hive.strict.checks.cartesian.product = false;
WITH sim_rank_table AS(
    SELECT 
    uid1,uid2,
    SUM(all_flag) - SUM(cmn_flag) AS all_cnt,
    SUM(cmn_flag) AS cmn_cnt,
    SUM(cmn_flag)/ (SUM(all_flag) - SUM(cmn_flag)) AS sim,
    ROW_NUMBER() over (PARTITION BY uid1 ORDER BY SUM(cmn_flag)/ (SUM(all_flag) - SUM(cmn_flag)) DESC) as sim_rank
FROM (SELECT  
        data1.user_id AS uid1,
        data1.movie_id AS mid1,
        data2.user_id AS uid2,
        data2.movie_id AS mid2,
        IF(data1.movie_id==data2.movie_id,1,0) AS cmn_flag,
        IF(data1.user_id==data2.user_id,1,0) AS all_flag 
    FROM movielens_test AS data1
    LEFT JOIN movielens_test AS data2 ON data1.movie_id = data2.movie_id
    ) cross_table
GROUP BY uid1,uid2)
--
SELECT uid1,COLLECT_LIST(uid2),COLLECT_LIST(sim_rank)
FROM sim_rank_table
WHERE CAST(uid1 AS STRING) LIKE CONCAT("%","1") AND sim_rank<=3
GROUP BY uid1;