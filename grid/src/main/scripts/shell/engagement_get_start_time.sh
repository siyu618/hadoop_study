#!/bin/bash

basePath=$(engagementBaseOutputPath)
# TODO : do we need to loop back here
backtrack_days=$(engagementBackTrackDays)

get_daily_job_start_time() {
	local base_path=$1;
	local look_back_days=$2;
	local done_file_pattern=${base_path}/
	local last_done_file_date=`hadoop fs -ls ${done_file_pattern} | tail -1 | awk -F/ '{index1=NF-1; if (index1 > 0){print $NF;} else {print "";}}'`;
	local start_time="";
	if [ -z $last_done_file_date ]; then
		start_time=`date +%Y-%m-%dT00:00Z`
	else
		start_time=`date -d"$last_done_file_date +2 days" +%Y-%m-%dT00:00Z`
	fi
	echo ${start_time};
}

get_daily_job_start_time ${basePath}