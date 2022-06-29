data1 = LOAD './movielens_small' USING PigStorage(',') AS (userid1:int,movieid1:int);
data2 = LOAD './movielens_small' USING PigStorage(',') AS (userid2:int,movieid2:int);
data_cross = CROSS data1,data2;
cross_filtered = FILTER data_cross BY userid1!=userid2;
cross_groups = GROUP cross_filtered BY (userid1,userid2);
common_count = FOREACH cross_groups{
		common_movies = FILTER cross_filtered BY movieid1==movieid2;
		common_cnt = COUNT(common_movies);
		GENERATE FLATTEN(group),common_cnt AS common_cnt;
}
common_count_desc = ORDER common_count BY common_cnt DESC;
res = LIMIT common_count_desc 10; 
DUMP res;
STORE res INTO './movielens_small_res1' USING PigStorage ('\t');
