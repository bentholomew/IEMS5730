data = LOAD 'hdfs://master:54310/user/hduser/gbook-all-res' 
	USING PigStorage('\t') 
	AS (bigram:chararray,year:int,match_count:int,volume_count:int);
data_groups = GROUP data BY bigram;
occ_per_year = FOREACH data_groups{
				unique_year = DISTINCT data.year;
				year_count = COUNT(unique_year);
				total_match_count = SUM(data.match_count);
				GENERATE group, (float)total_match_count/year_count AS avg_match_count;
			   }
occ_per_year_desc = ORDER occ_per_year BY avg_match_count DESC;
result = LIMIT occ_per_year_desc 20;
DUMP result;
STORE occ_per_year INTO 'hdfs://master:54310/user/hduser/pig_result' USING PigStorage('\t');