drop schema if exists ${tmpDatabase} cascade;
create schema ${tmpDatabase} location '${tmpDatabaseLocation}';
use ${tmpDatabase};

CREATE EXTERNAL TABLE campaign_event_external
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION  '${OUTPUT}'
TBLPROPERTIES ('avro.schema.literal' = '
{
  "namespace": "com.yahoo.pt_exposure_report.avro",
  "type": "record",
  "name": "CampaignEvent",
  "fields": [
    {
      "name": "SID",
      "type": [
        "string",
        "null"
      ]
    },
    {
      "name": "BID",
      "type": [
        "string",
        "null"
      ]
    },
    {
      "name": "IO",
      "type": [
        "string",
        "null"
      ]
    },
    {
      "name": "DATE",
      "type": [
        "string",
        "null"
      ]
    },
    {
      "name": "PROPERTY_BOOKING",
      "type": [
        "string",
        "null"
      ]
    },
    {
      "name": "SITE",
      "type": [
        "string",
        "null"
      ],
      "doc": "In fact it is SITE NAME"
    },
    {
      "name": "POSITION",
      "type": [
        "string",
        "null"
      ]
    },
    {
      "name": "LINE",
      "type": [
        "string",
        "null"
      ],
      "doc": "It has another name ad_grp_placement_id"
    },
    {
      "name": "DEVICE",
      "type": [
        "string",
        "null"
      ],
      "doc": "In fact it is the device name."
    },
    {
      "name": "OS",
      "type": [
        "string",
        "null"
      ],
      "doc": "In fact it is the operating system name."
    },
    {
      "name": "BROWSER",
      "type": [
        "string",
        "null"
      ],
      "doc": "In fact it is the browser name."
    },
    {
      "name": "CARRIER",
      "type": [
        "string",
        "null"
      ],
      "doc": "In fact it is the carrier name."
    },
    {
      "name": "CREATIVEID",
      "type": [
        "string",
        "null"
      ]
    },
    {
      "name": "IMPRESSIONS",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "CLICKS",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "SITEID",
      "type": [
        "string",
        "null"
      ]
    },
    {
      "name": "DEVICEID",
      "type": [
        "string",
        "null"
      ]
    },
    {
      "name": "OSID",
      "type": [
        "string",
        "null"
      ]
    },
    {
      "name": "BROWSERID",
      "type": [
        "string",
        "null"
      ]
    },
    {
      "name": "CARRIERID",
      "type": [
        "string",
        "null"
      ]
    }
  ]
}
');

create view if not exists latest_b2smap(bid, sid, m_date) as
    select bid, sid, m_date from ${database}.btosmap where ${btosmap_filter};

create view if not exists  b2s(bid, sid) as
    select a.bid, a.sid from latest_b2smap a inner join (
        select bid, max(m_date) as m_date from latest_b2smap group by bid) b
    on a.bid = b.bid and a.m_date = b.m_date;

create view if not exists line_mapping( line, pos, property_booking ) as
    select cast(src_adv_order_line_id as string), targ_ad_dlvr_mode_name_text, targ_site_name_text from ${database}.apt_d_demand_hierarchy where ${apt_d_demand_hierarchy_filter};

create view if not exists mobil_device_id_mapping( mobl_device_id, mobl_device_name ) as
    select cast(src_device_id as string), device_path from ${database}.apt_d_device where ${apt_d_device_filter};

create view if not exists operating_system_id_mapping(operating_system_id, operating_system_name) as
    select src_operating_system_id, operating_system_name from ${database}.apt_d_operating_system where ${apt_d_operating_system_filter};

create view if not exists browser_id_mapping(browser_id, browser_name) as
    select cast(src_browser_id as string), browser_name from ${database}.apt_d_browser where ${apt_d_browser_filter};

create view if not exists mobl_carrier_id_mapping(mobl_carrier_id, mobl_carrier_name) as
    select cast(src_connect_provider_id as string), connect_provider_name from ${database}.apt_d_connect_provider where ${apt_d_connect_provider_filter};

create view if not exists site_id_mapping(site_id, site_name) as
    select cast(src_site_id as string), site_name from ${database}.apt_d_site where ${apt_d_site_filter};


--
--    BID, CREATIVE_ID, LINE, DATE, IO, SITE,
--    DeviceName,
--    OS Name, Browser Name, carrier,
--    IMPRESSIONS, CLICKS
--

create view if not exists  my_apt_agg(
        bid, adv_creative_id, line,
        ts, order_id, site_id, mobl_device_id,
        operating_system_id, browser_id, mobl_carrier_id,
        impressions, clicks) as
    select user_id as bid, adv_creative_id, ad_grp_placement_id as line, substring(ts, 1, 8) as ts, order_id, site_id,
        mobl_device_id,
        operating_system_id, browser_id, mobl_carrier_id,
        impressions, clicks
    from ${database}.apt_agg where (${aptagg_partition_filter}) and (${aptagg_line_filter}) ;

--  need to mapping
--     bid --> sid
--     line --> pos & PropertyBooking
--     mobl_device_id --> mobl_device_name
--     operating_system_id --> operating_system_name
--     browser_id --> browser_name
--     mobl_carrier_id --> mobl_carrier_name
--     site_id --> site_name
-- TODO : for optimization, join ==> map side join, small table first
--
--     bid --> sid
create view if not exists  my_apt_agg_bid_mapping (
        bid, adv_creative_id, line,
        ts, order_id, site_id, mobl_device_id,
        operating_system_id, browser_id, mobl_carrier_id,
        impressions, clicks,
        sid
        )as
   select a.bid ,
            a.adv_creative_id, a.line, a.ts,
            a.order_id, a.site_id, a.mobl_device_id,
            a.operating_system_id, a.browser_id,
            a.mobl_carrier_id, a.impressions, a.clicks,
            b.sid
   from my_apt_agg a left outer join b2s b
   on a.bid = b.bid;

--     line --> pos & PropertyBooking
create view if not exists  my_apt_agg_line_mapping (
        bid, adv_creative_id, line,
        ts, order_id, site_id, mobl_device_id,
        operating_system_id, browser_id, mobl_carrier_id,
        impressions, clicks,
        sid,
        pos, property_booking
        ) as
   select a.bid ,
            a.adv_creative_id, a.line, a.ts,
            a.order_id, a.site_id, a.mobl_device_id,
            a.operating_system_id, a.browser_id,
            a.mobl_carrier_id, a.impressions, a.clicks,
            a.sid,
            b.pos, b.property_booking
   from my_apt_agg_bid_mapping a left outer join line_mapping b
   on a.line = b.line;

--     mobl_device_id --> mobl_device_name
create view if not exists  my_apt_agg_mobil_device_id_mapping (
        bid, adv_creative_id, line,
        ts, order_id, site_id, mobl_device_id,
        operating_system_id, browser_id, mobl_carrier_id,
        impressions, clicks,
        sid,
        pos, property_booking,
        mobl_device_name
        ) as
   select a.bid ,
            a.adv_creative_id, a.line, a.ts,
            a.order_id, a.site_id, a.mobl_device_id,
            a.operating_system_id, a.browser_id,
            a.mobl_carrier_id, a.impressions, a.clicks,
            a.sid,
            a.pos, a.property_booking,
            b.mobl_device_name
   from my_apt_agg_line_mapping a left outer join mobil_device_id_mapping b
   on a.mobl_device_id = b.mobl_device_id;

--     operating_system_id --> operating_system_name
create view if not exists  my_apt_agg_operating_system_id_mapping (
        bid, adv_creative_id, line,
        ts, order_id, site_id, mobl_device_id,
        operating_system_id, browser_id, mobl_carrier_id,
        impressions, clicks,
        sid,
        pos, property_booking,
        mobl_device_name,
        operating_system_name
        ) as
   select a.bid ,
            a.adv_creative_id, a.line, a.ts,
            a.order_id, a.site_id, a.mobl_device_id,
            a.operating_system_id, a.browser_id,
            a.mobl_carrier_id, a.impressions, a.clicks,
            a.sid,
            a.pos, a.property_booking,
            a.mobl_device_name,
            b.operating_system_name
   from my_apt_agg_mobil_device_id_mapping a left outer join operating_system_id_mapping b
   on a.operating_system_id = b.operating_system_id;

--     browser_id --> browser_name
create view if not exists  my_apt_agg_browser_id_mapping (
        bid, adv_creative_id, line,
        ts, order_id, site_id, mobl_device_id,
        operating_system_id, browser_id, mobl_carrier_id,
        impressions, clicks,
        sid,
        pos, property_booking,
        mobl_device_name,
        operating_system_name,
        browser_name
        ) as
   select a.bid ,
            a.adv_creative_id, a.line, a.ts,
            a.order_id, a.site_id, a.mobl_device_id,
            a.operating_system_id, a.browser_id,
            a.mobl_carrier_id, a.impressions, a.clicks,
            a.sid,
            a.pos, a.property_booking,
            a.mobl_device_name,
            a.operating_system_name,
            b.browser_name
   from my_apt_agg_operating_system_id_mapping a left outer join browser_id_mapping b
   on a.browser_id = b.browser_id;

--     mobl_carrier_id --> mobl_carrier_name
create view if not exists  my_apt_mobl_carrier_id_mapping (
        bid, adv_creative_id, line,
        ts, order_id, site_id, mobl_device_id,
        operating_system_id, browser_id, mobl_carrier_id,
        impressions, clicks,
        sid,
        pos, property_booking,
        mobl_device_name,
        operating_system_name,
        browser_name,
        mobl_carrier_name
        ) as
   select a.bid ,
            a.adv_creative_id, a.line, a.ts,
            a.order_id, a.site_id, a.mobl_device_id,
            a.operating_system_id, a.browser_id,
            a.mobl_carrier_id, a.impressions, a.clicks,
            a.sid,
            a.pos, a.property_booking,
            a.mobl_device_name,
            a.operating_system_name,
            a.browser_name,
            b.mobl_carrier_name
   from my_apt_agg_browser_id_mapping a left outer join mobl_carrier_id_mapping b
   on a.mobl_carrier_id = b.mobl_carrier_id;

--     site_id --> site_name
create view if not exists  my_apt_mobl_site_id_mapping (
        bid, adv_creative_id, line,
        ts, order_id, site_id, mobl_device_id,
        operating_system_id, browser_id, mobl_carrier_id,
        impressions, clicks,
        sid,
        pos, property_booking,
        mobl_device_name,
        operating_system_name,
        browser_name,
        mobl_carrier_name,
        site_name
        ) as
   select a.bid ,
            a.adv_creative_id, a.line, a.ts,
            a.order_id, a.site_id, a.mobl_device_id,
            a.operating_system_id, a.browser_id,
            a.mobl_carrier_id, a.impressions, a.clicks,
            a.sid,
            a.pos, a.property_booking,
            a.mobl_device_name,
            a.operating_system_name,
            a.browser_name,
            a.mobl_carrier_name,
            b.site_name
   from my_apt_mobl_carrier_id_mapping a left outer join site_id_mapping b
   on a.site_id = b.site_id;


INSERT  INTO TABLE ${tmpDatabase}.campaign_event_external
   select sid, bid, order_id, ts,
        property_booking, site_name, pos,
        line, mobl_device_name,
        operating_system_name, browser_name,
        mobl_carrier_name, adv_creative_id,
        impressions, clicks,
        site_id, mobl_device_id,
        operating_system_id,
        browser_id, mobl_carrier_id
   from my_apt_mobl_site_id_mapping;


drop view if exists latest_b2smap;
drop view if exists b2s;

drop view if exists line_mapping;
drop view if exists mobil_device_id_mapping;
drop view if exists operating_system_id_mapping;
drop view if exists browser_id_mapping;
drop view if exists mobl_carrier_id_mapping;

drop view if exists my_apt_agg;
drop view if exists my_apt_agg_bid_mapping;
drop view if exists my_apt_agg_line_mapping;
drop view if exists my_apt_agg_mobil_device_id_mapping;
drop view if exists my_apt_agg_operating_system_id_mapping;
drop view if exists my_apt_agg_browser_id_mapping;
drop view if exists my_apt_mobl_carrier_id_mapping;
drop view if exists my_apt_mobl_site_id_mapping;
drop schema if exists ${tmpDatabase} cascade;