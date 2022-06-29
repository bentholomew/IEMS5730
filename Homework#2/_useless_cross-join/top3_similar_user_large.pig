data1 = LOAD './movielens_large' USING PigStorage(',') AS (userid1:int,movieid1:int);
data2 = LOAD './movielens_large' USING PigStorage(',') AS (userid2:int,movieid2:int);
cross_data = CROSS data1,data2;

cross_filtered = FILTER cross_data BY userid1!=userid2;
cross_groups = GROUP cross_filtered BY (userid1,userid2);
sim_count = FOREACH cross_groups{
		common_movies = FILTER cross_filtered BY movieid1==movieid2;
		common_cnt = COUNT(common_movies);
		all_cnt = COUNT(cross_filtered.movieid1)-common_cnt;
		similarities = (float)common_cnt/all_cnt; 
		GENERATE FLATTEN(group),similarities AS similarities;
}
sim_count_groups = GROUP sim_count BY userid1;
res = FOREACH sim_count_groups {
		user_rank = ORDER sim_count BY similarities DESC;
		top3 = LIMIT user_rank 3;
		top3_userid = FOREACH top3 GENERATE userid2;
        GENERATE FLATTEN(group) AS userid,top3_userid;
}


my_res = FOREACH res GENERATE (chararray)userid, top3_userid;
my_res = FILTER my_res BY ENDSWITH(userid,'35');
DUMP my_res;
STORE my_res INTO 'movielens_large_res2' USING PigStorage ('\t');