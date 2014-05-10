register $PIG_BANK;

campaign_event = load '$campaign_event' using org.apache.pig.piggybank.storage.avro.AvroStorage();
line_campaign_event = foreach campaign_event
    generate
    LINE as line,
    IMPRESSIONS as impressions,
    CLICKS as clicks;

line_info = load '$line_io_info' using PigStorage('|');
line_io_distinct =
    foreach line_info
    generate
    $0 as line,
    $3 as ad_system;


line_campaign_event_g = group line_campaign_event by line;
line_campaign_event_agg =
    foreach line_campaign_event_g
    generate
    group as line,
    SUM(line_campaign_event.impressions) as impressions,
    SUM(line_campaign_event.clicks) as clicks;

line_io_event_join =
    join
    line_io_distinct by line LEFT OUTER,
    line_campaign_event_agg by line;

line_io_stats =
    foreach line_io_event_join
    generate
    line_io_distinct::line as line,
    line_io_distinct::ad_system as ad_system,
    (line_campaign_event_agg::impressions is not null ? line_campaign_event_agg::impressions  : 0 ) as impressions,
    (line_campaign_event_agg::clicks is not null ? line_campaign_event_agg::clicks : 0 ) as clicks;

store line_io_stats into '$output' using PigStorage('|');

