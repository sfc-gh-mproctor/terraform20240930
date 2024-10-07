-------------------------------------------------
-- NAME:	 WH-AU-Query-and-Warehouse-Workload-Analysis.sql
-------------------------------------------------
-- DESCRIPTION:
-- This query is the kitchen sink of workload query analysis. Tons of metrics useful for many purposes, see use cases below.
-- Sort algorithm used: Find most expensive queries based on MP count above the P95 value 
--        * exec sec over P95 * remote spill over P95 * MP count over P99 * If WH timeout * 100
-- Comparing WH, WH size, query_type, and day to stats of queries grouped by
--		day, WH, WH size, execution_status, query_type, and query_parameterized_hash
--
-- POSSIBLE USE CASES:

--  You can see side-by-side the stats of the same query_parameterized_hash next to P95 stats of the WH for similar query_type on same size WH.
--  Finding poor partition eliminiation queries
--  Find high compile time queries and compare to WH P95 values.
--  Enables you to compare workloads after WH size change.
--	Enables you to find most expensive queries per query type, WH, and WH size.
--	Figure out what size WH should be used based on MP count (the ...ISH columns for approx size WH)
--  Query Acceleration Service (QAS) usage or recommendations.
--  View queue times for the hash query and WH size.
--
-- OUTPUT:
--	See inline comments on columns, but useful metrics to help do analysis of workloads.
--  If analyzing many WHs, enable the QUALIFY clause to do a top 15 query to not overwhelm yourself.

--
-- NEXT STEPS:
--	Depending on your use case, you could do many things. This kind enables too many next steps to document.
--  If QAS is recommended, see QAS sizing recommendation script.
--
-- OPTIONS:
--	Change timeframe window to within your controlled experiment time
--	Alter the Sort algorithm (perf_score) to suit your environment/needs
--	Change Warehouse_name parameter to name you use or hard code to a value.
--  Enable Qualify clause to limit to top x of WH and WH size. Change this to suit your needs.
--
-- REVISION HISTORY
-- DATE			INIT	DESCRIPTION
----------  	----    -----------
-- 2023-11-01	ChuckL	CREATED
-- 2023-11-09	ChuckL	Documented, added dense_rank function, added WH query efficiency column
-------------------------------------------------


call pst.svcs.sp_set_account_context (7013,'prod1'); --Dropbox
use schema pst.svcs;

set warehouse_name = 'RDE';

WITH timewindow
AS (
	-- adjust this start_marker to globally alter the "lookback" window for the rest of the CTE's
	SELECT to_timestamp(dateadd(day, - 3, CURRENT_DATE ())) AS start_marker
	)
, pc AS (
	SELECT date_trunc('day', start_time) day --assume there is not execution time skew across a day, else shift to hour.
		, warehouse_name
		, warehouse_size
		, query_type
		, count(*) AS WH_Query_Count_by_type
		, round(percentile_cont(.95) within GROUP (
				ORDER BY execution_time / 1000
				), 1) AS wh_exec_sec_95th
		, round(percentile_cont(.95) within GROUP (
				ORDER BY compilation_time / 1000
				), 1) AS wh_comp_sec_95th
		, round(percentile_cont(.99) within GROUP (
				ORDER BY partitions_scanned
				), 1) AS wh_part_scanned_99th
		, round(percentile_cont(.95) within GROUP (
				ORDER BY queued_overload_time
				), 1) AS wh_queued_time_95th
	FROM query_history qh
	JOIN timewindow tw
	WHERE true
		AND start_time >= tw.start_marker
		AND execution_status = 'SUCCESS'
		AND query_type <> 'CALL' --Minor 1 cpu management of child queries, so excluding
		AND cluster_number > 0 -- Used WH compute
		AND qh.warehouse_name = $warehouse_name
	--AND user_name = upper('')
	GROUP BY day
		, warehouse_name
		, warehouse_size
		, query_type
		--order by 1,2
	)
SELECT date_trunc('day', qh.start_time) AS day
	, any_value(user_name) user_name
	, any_value(role_name) role_name
	, any_value(database_name) database_name --Just part of the connection use info/task schema
	, any_value(schema_name) schema_name --Just part of the connection use info/task schema
	, qh.execution_status
	, any_value(error_code) AS error_code
	, any_value(error_message) error_message
	, qh.warehouse_name
	, qh.warehouse_size
    , case when qh.warehouse_type = 'STANDARD' THEN 
        decode(qh.warehouse_size, 'X-Small', 1, 'Small', 2, 'Medium', 4, 'Large', 8, 'X-Large', 16, '2X-Large', 32, '3X-Large', 64, '4X-Large', 128, '5X-Large', 256, '6X-Large', 512, null)
      else decode(qh.warehouse_size, 'Medium', 6, 'Large', 12, 'X-Large', 24, '2X-Large', 48, '3X-Large', 96, '4X-Large', 192, '5X-Large', 384, '6X-Large', 768, null) as nodes_avail
    , round(avg(query_load_percent/100*nodes_avail),1) avg_query_WH_efficiency
	--, total_elapsed_time/1000 
	-- , round(avg(execution_time)/1000,1) as avg_execution_time_sec
	-- , CASE
	--         WHEN (avg_execution_time_sec) <= 15 THEN 'A-0s-15s'
	--         WHEN (avg_execution_time_sec) <= 45 THEN 'B-15s-45s'
	--         WHEN (avg_execution_time_sec) <= 60 THEN 'C-45s-60s'
	--         WHEN (avg_execution_time_sec) <= 240 THEN 'D-60s-4mins'
	--         WHEN (avg_execution_time_sec) <= 1800 THEN 'E-4mins-30mins'
	--         ELSE 'F-30min+'
	--     END as Avg_Exec_TimeBucket --Avg is really affected by outliers, P95 is a better stat, or P90.
	, qh.query_type --This is useful to see as operations like DELETE should not be compared/data mixed with SELECTs for instance.
	, count(*) AS QC_by_query_hash_query_type --QC = Query Count.
	, any_value(WH_Query_Count_by_type) AS WH_QC_by_type --Consider ignoring 95th exec time if query count is low.
	, round(max(execution_time) / 1000, 1) AS max_execution_time_sec
    , any_value(WH_Exec_sec_95th) AS WH_Exec_sec_95th
    , case when round(max(execution_time / 1000 - WH_Exec_sec_95th)) > 0 then round(max(execution_time / 1000 - WH_Exec_sec_95th))
        else 0 end AS max_exec_sec_over_wh_p95
	, round(percentile_cont(.95) within GROUP (
			ORDER BY execution_time / 1000
			), 1) AS p95_exec_sec
    -- Use this exec time bucket in graphing. May need to adjust for longer running WH's.
    , CASE
        WHEN (p95_exec_sec) <= 15 THEN 'A-0s-15s'
        WHEN (p95_exec_sec) <= 45 THEN 'B-15s-45s'
        WHEN (p95_exec_sec) <= 60 THEN 'C-45s-60s'
        WHEN (p95_exec_sec) <= 240 THEN 'D-60s-4mins'
        WHEN (p95_exec_sec) <= 1800 THEN 'E-4mins-30mins'
        ELSE 'F-30min+'
    END as P_95_Exec_TimeBucket 

    --Partition count
    , max(partitions_total) max_partitions_total
    , max(partitions_scanned) max_partitions_scanned
    --Partition counts vary widely, so using 99th percentile value.
    , any_value(wh_part_scanned_99th) AS wh_part_scanned_99th
    , case when max(partitions_scanned - wh_part_scanned_99th) > 0 
        then round(max(partitions_scanned - wh_part_scanned_99th)) else 0 end AS max_part_scanned_over_wh_p99
    , CASE WHEN max_partitions_total > 20
            AND max_partitions_total > 0 --Ignore small often metadata like queries
            THEN round(percentile_cont(.95) within GROUP (
                        ORDER BY (partitions_total - partitions_scanned) / greatest(partitions_total, 1)
                        ) * 100, 1) ELSE NULL END AS query_partition_pruning_perc_p95
    --These are just starting points:
    , SUM(CASE WHEN partitions_scanned < 8000 THEN 1 ELSE 0 END) AS SMALLISH_QC
	, SUM(CASE WHEN partitions_scanned < 32000
				AND partitions_scanned >= 8000 THEN 1 ELSE 0 END) AS MEDIUMISH_QC
	, SUM(CASE WHEN partitions_scanned < 128000
				AND partitions_scanned >= 32000 THEN 1 ELSE 0 END) AS XLARGEISH_QC
	, SUM(CASE WHEN partitions_scanned < 512000
				AND partitions_scanned >= 128000 THEN 1 ELSE 0 END) AS X3LARGEISH_QC
	, SUM(CASE WHEN partitions_scanned >= 512000 THEN 1 ELSE 0 END) AS X5LARGEISH_QC

    --Queuing on WH - You want a small amount of queuing to have efficient use of the WH. Too much on an ad-hoc WH and user's will complain. 
    --Add Multi-cluster WH capability to handle concurrency and lower this value to an acceptable level.
    , round(percentile_cont(.95) within GROUP (
        ORDER BY queued_overload_time / 1000
        ), 1) AS queue_sec_p95
	, round(any_value(wh_queued_time_95th)/1000,1) AS wh_queue_sec_95th

	--QAS - Query Acceleration Service to help offload SELECT operation large table scans to other WH's provided as a service.
	, max(query_acceleration_partitions_scanned) max_qas_partitions_scanned
    , max(query_acceleration_bytes_scanned) MAX_QAS_BYTES_SCANNED
	, round(sum(qas.eligible_query_acceleration_time) / 1000) AS sum_qas_eligible_time_sec_total
	, round(avg(qas.upper_limit_scale_factor)) AS qas_eligible_avg_scale_factor
	--, median(qas.upper_limit_scale_factor) as qas_median_scale_factors
    
    --Compiliation time -- If high, see if static values used and move into a table, or remove complexity of nested views/subqueries/cte's with materialized temp tables in a pipeline.
	, round(avg(compilation_time) / 1000, 1) AS avg_compile_time_sec
	, round(percentile_cont(.95) within GROUP (
			ORDER BY compilation_time / 1000
			), 1) AS compile_time_sec_95th
	, any_value(wh_comp_sec_95th) AS WH_Compile_time_sec_95th
	
    --WH server memory effecting stats
	, round(max(bytes_spilled_to_local_storage) / power(1024, 2)) max_Mbytes_spilled_to_local_storage -- No real action here
	, round(max(bytes_spilled_to_remote_storage) / power(1024, 2)) max_Mbytes_spilled_to_remote_storage --If remote storage hit, QAS is needed or larger WH needed.

    --Interesting data point for cases when row count is much higher than expected and deeper analysis with RSA is needed.
	, max(rows_produced) max_rows_produced
	
    --Prioritize investigation by anomolous WH use by MP count over P99 of queries of same query_type, using this sort prioritization alogrithm:
	, round(QC_by_query_hash_query_type * (greatest(1, max_exec_sec_over_wh_p95)) --Over P95 execution time of same query type on same WH on same day, so suspect
		* greatest(1, max_part_scanned_over_wh_p99) --Over P99 for partition count of same query type on same WH on same day, so suspect
		* greatest(1, max_Mbytes_spilled_to_remote_storage) --If remote storage hit, QAS is needed or larger WH needed.
		* IFF(any_value(error_code) = '000630', 100, 1)) --Hit WH timeout. Query optimization and/or larger WH, it is possible QAS could help, see QAS sum_qas_eligible_time_sec_total
	AS Perf_Score
    --Other estimates of how efficient the query is at using all the nodes in a WH and product of that and exec time as another sort option to find inefficient queries.
    , round(avg(query_load_percent/100*nodes_avail*execution_time*1000),1) as avg_compute_load_sec
    , dense_rank() over (partition by qh.warehouse_name, qh.warehouse_size order by Perf_Score desc, avg_compute_load_sec desc) as WH_SIZE_RANK
	, qh.query_parameterized_hash
	, any_value(qh.query_id) Example_Query_id
	, 'https://app.snowflake.com/<addr>/prod/#/compute/history/queries/' || any_value(qh.query_id) AS Query_Profile_Page
	, any_value(left(qh.query_text, 400)) query_text_400_characters
FROM query_history qh
JOIN pc ON pc.day = date_trunc('day', qh.start_time)
	AND pc.warehouse_name = qh.warehouse_name
	AND pc.warehouse_size = qh.warehouse_size
	AND pc.query_type = qh.query_type
JOIN timewindow tw
--If you haven't enabled QAS on this server yet, you can see eligible queries:
LEFT JOIN query_acceleration_eligible qas ON qas.query_id = qh.query_id
	AND qas.start_time >= tw.start_marker
WHERE true
	AND qh.start_time >= tw.start_marker
	AND qh.query_type <> 'CALL' --Minor 1 cpu management of child queries, so excluding
	--and qh.user_name <> 'SYSTEM' --Exclude tasks
	--and user_name = ''
	--and qh.schema_name like '%%'
	--and database_name = ''
	--and qh.warehouse_name = ''
	AND qh.warehouse_name = $Warehouse_Name
	--and execution_status <> 'SUCCESS'
	AND qh.cluster_number > 0 --Exclude cloud services WH
GROUP BY date_trunc('day', qh.start_time)
	, qh.warehouse_name
	, qh.warehouse_size
	, qh.execution_status
	, qh.query_type
	, qh.query_parameterized_hash
--QUALIFY dense_rank() over (partition by qh.warehouse_name, qh.warehouse_size order by Perf_Score desc, avg_compute_load_sec desc) <= 15 --See top 15 per WH and Size.
ORDER BY qh.warehouse_name
	, WH_SIZE_RANK
;