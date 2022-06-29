CREATE TABLE movielens_small(
    user_id INT,
    movie_id INT)
COMMENT 'hw2_movielens_small'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

LOAD DATA INPATH './movielens_small' INTO TABLE movielens_small;

SET hive.strict.checks.cartesian.product = false;

WITH sim_rank_table AS(
    SELECT 
    uid1,uid2,
    COUNT(*)-SUM(cmn_flag) AS all_cnt,
    SUM(cmn_flag) AS cmn_cnt,
    SUM(cmn_flag)/ (COUNT(*)-SUM(cmn_flag)) AS sim,
    ROW_NUMBER() over (PARTITION BY uid1 ORDER BY SUM(cmn_flag)/ (COUNT(*)-SUM(cmn_flag)) DESC) as sim_rank
FROM (SELECT  
        data1.user_id AS uid1,
        data1.movie_id AS mid1,
        data2.user_id AS uid2,
        data2.movie_id AS mid2,
        IF(data1.movie_id==data2.movie_id,1,0) AS cmn_flag
    FROM movielens_small AS data1
    CROSS JOIN movielens_small AS data2
    WHERE data1.user_id != data2.user_id) cross_table
GROUP BY uid1,uid2)

SELECT col1.uid, sim_uid1, sim_uid2, sim_uid3 FROM
(SELECT uid1 AS uid, uid2 AS sim_uid1
FROM sim_rank_table WHERE sim_rank == 1) col1
INNER JOIN
(SELECT uid1 AS uid, uid2 AS sim_uid2
FROM sim_rank_table WHERE sim_rank == 2) col2 ON col1.uid=col2.uid
INNER JOIN
(SELECT uid1 AS uid,uid2 AS sim_uid3
FROM sim_rank_table WHERE sim_rank == 3) col3 ON col1.uid=col3.uid
WHERE CAST(col1.uid AS STRING) LIKE CONCAT("%","35");