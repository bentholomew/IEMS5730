/************************************************************************	
    HW2 Q3(b): SELECTING THE TOP3 MOST SIMILAR USERS OF EACH USER
    SID: 1155162635
*************************************************************************/


/*** 
    STEP#1: Calculate the common movies shared by each pair of the users like Q3(b)
    OUTPUT: (user1,user2,common_cnt)
***/
-- #1.1: LOAD THE DATA: 2 Copies
data1 = LOAD 'movielens_test' USING PigStorage(',') AS (userid1:int,movieid1:int);
data2 = LOAD 'movielens_test' USING PigStorage(',') AS (userid2:int,movieid2:int);

-- #1.2: SELF INNER JOIN ON `MOVIES`: to compute each pair of fans of any movie 
cmn_movies = JOIN data1 by movieid1, data2 BY movieid2;
cmn_movies_notself = FILTER cmn_movies BY userid1!=userid2;

-- #1.3: GROUP THE DATA: (user1,user2,{bag of common movies watched})
cmn_movies_groups = GROUP cmn_movies_notself BY (userid1,userid2);
cmn_cnt_res = FOREACH cmn_movies_groups GENERATE FLATTEN(group) ,COUNT(cmn_movies_notself.movieid1) AS common_cnt;



/*** 
    STEP#2: Calculate the similarities score of the above user pairs
    OUTPUT: (user1,user2,sim) 
***/
-- #2.1: Calculate the movies sum of each user
user_groups = GROUP data1 BY userid1;
users = FOREACH user_groups GENERATE group AS userid,COUNT(data1.movieid1) AS user_cnt;

-- #2.2: Look for the movies sum of user1&user2 by performing 2 times of LEFT OUTER JOIN
with_user1_cnt = JOIN cmn_cnt_res by userid1 LEFT OUTER, users BY userid;
with_user2_cnt = JOIN with_user1_cnt by userid2 LEFT OUTER, users BY userid;

-- #2.3: Calculate the similarities with inclusion-exclusion priciple
sim_res = FOREACH with_user2_cnt {
			all_cnt = with_user1_cnt::users::user_cnt + users::user_cnt - common_cnt;
			similarities = (float)common_cnt/all_cnt;
			GENERATE userid1, userid2, similarities AS sim;
}



/***
    STEP#3: Rank the similarity score out select the top#3 most similar users of each user
    OUTPUT: (user1,sim_user#1,sim_user#2,sim_user#3)    
***/
-- #3.1: Group by userid1, then rank userid2 by their similarity score
sim_res_groups = GROUP sim_res BY userid1;
res =   FOREACH sim_res_groups {
		user_rank = ORDER sim_res BY sim DESC;
		top3 = LIMIT user_rank 3;
		top3_userid = FOREACH top3 GENERATE userid2;
        GENERATE FLATTEN(group) AS userid,top3_userid;
}

-- #3.2: Convert userid into STRING, selecting the results ends with '35'
user_list = FOREACH data1 GENERATE userid1;
user_bag = DISTINCT user_list;
res = JOIN user_bag BY userid1 LEFT OUTER, res BY userid;
my_res = FOREACH res GENERATE (chararray)userid, top3_userid;
DUMP my_res;
STORE my_res INTO './top3_sim_small' USING PigStorage('\t');
