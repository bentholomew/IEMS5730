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
    COUNT(*) AS all_cnt,
    SUM(cmn_flag) AS cmn_cnt,
    SUM(cmn_flag)/ COUNT(*) AS sim,
    RANK() over (PARTITION BY uid1 ORDER BY SUM(cmn_flag)/ COUNT(*) DESC) as sim_rank
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

SELECT * FROM sim_rank_table WHERE sim_rank<=3 AND CAST(uid1 AS STRING) LIKE CONCAT("%","35")