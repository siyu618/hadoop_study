-- create external table in a tmp database
--    : when drop schema, the data of external table will not be dropped.
drop schema if exists ${tmpDatabase} cascade;
create schema ${tmpDatabase} location '${tmpDatabaseLocation}';
use ${tmpDatabase};

CREATE EXTERNAL TABLE user_engagement_external
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION  '${OUTPUT}'
TBLPROPERTIES ('avro.schema.literal' = '
{
  "namespace": "com.yahoo.pt_exposure_report.avro",
  "type": "record",
  "name": "UserEngagement",
  "fields": [
    {
      "name": "SID",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "name": "BID",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "name": "DATE",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "name": "NETWORK_IMPRESSIONS",
      "type": [
        "null",
        "long"
      ]
    },
    {
      "name": "NETWORK_ADCLICKS",
      "type": [
        "null",
        "long"
      ]
    },
    {
      "name": "MOBILE_NETWORK_IMPRESSIONS",
      "type": [
        "null",
        "long"
      ]
    },
    {
      "name": "MOBILE_NETWORK_ADCLICKS",
      "type": [
        "null",
        "long"
      ]
    }
  ]
}
');

-- create
drop view if exists latest_b2smap;
drop view if exists b2s;
drop view if exists my_apt_agg;

create view if not exists latest_b2smap(bid, sid, m_date) as
select bid, sid, m_date from ${database}.btosmap where ${b2s_filter};

create view if not exists  b2s(bid, sid) as
select a.bid, a.sid from latest_b2smap a inner join (
select bid, max(m_date) as m_date from latest_b2smap group by bid) b
on a.bid = b.bid and a.m_date = b.m_date;

create view if not exists  my_apt_agg(bid, ts, impressions, clicks, mobile_impressions, mobile_clicks) as
select user_id as bid, substring(ts, 1, 8) as ts, impressions, clicks,
    case when length(mobl_device_id) > 0 then impressions
    else cast('0' as bigint)
    end as mobile_impressions,
    case when length(mobl_device_id) > 0 then clicks
    else cast('0' as bigint)
    end as mobile_clicks
from ${database}.apt_agg where (${aptagg_filter});
-- and (length(user_id) == 13);
-- TODO : for optimization, join ==> map side join, small table first
INSERT  INTO TABLE ${tmpDatabase}.user_engagement_external
   select sid, bid, ts, sum(impressions), sum(clicks), sum(mobile_impressions), sum(mobile_clicks)
   from (
        select b.sid, a.bid, a.ts, a.impressions, a.clicks, a.mobile_impressions, a.mobile_clicks
        from my_apt_agg a left outer join b2s b
        on a.bid = b.bid
        ) t
   group by bid, sid, ts;

-- clean up
drop view if exists latest_b2smap;
drop view if exists b2s;
drop view if exists my_apt_agg;
drop schema if exists ${tmpDatabase} cascade;





