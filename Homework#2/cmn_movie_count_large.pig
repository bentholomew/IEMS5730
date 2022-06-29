/************************************************************************	
    HW2 Q3(a): CALCULATING COMMON MOVIES WATCHED BY PAIRS OF USERS
*************************************************************************/

-- LOAD THE DATA: 2 Copies
data1 = LOAD './movielens_large' USING PigStorage(',') AS (userid1:int,movieid1:int);
data2 = LOAD './movielens_large' USING PigStorage(',') AS (userid2:int,movieid2:int);

-- SELF INNER JOIN ON `MOVIES`: to compute each pair of fans of any movie 
cmn_movies = JOIN data1 by movieid1, data2 BY movieid2;
cmn_movies_notself = FILTER cmn_movies BY userid1!=userid2;

-- GROUP THE DATA: (user1,user2,{bag of common movies watched})
cmn_movies_groups = GROUP cmn_movies_notself BY (userid1,userid2);
res = FOREACH cmn_movies_groups GENERATE FLATTEN(group),COUNT(cmn_movies_notself.movieid1) AS common_cnt;

-- OUTPUT: order then select the top 10 pair of users who watch the most shared movies;
res = ORDER res BY common_cnt DESC;
res = LIMIT res 10;
STORE res INTO './common_large_res' USING PigStorage ('\t');