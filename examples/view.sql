CREATE DATABASE IF NOT EXISTS metrics;

CREATE TABLE metrics.example (
     DateTime  DateTime
    , Service  String
    , Label    String
    , ReqCount Int32
    , Duration Float64
    , Tags    Nested (
        Key String
        , Value String
    )
) Engine SummingMergeTree 
PARTITION BY (Service, toYYYYMM(DateTime))
ORDER BY (
    DateTime
    , Label
    , Tags.Key
    , Tags.Value
); 

CREATE MATERIALIZED VIEW metrics.example_mv TO metrics.example AS 
    SELECT 
        toDateTime(intDiv(toInt32(DateTime), 15)*15) AS DateTime
        , Service
        , Label
        , CAST(1 AS Int32) AS ReqCount
        , Value            AS Duration
        , Tags.Key         AS `Tags.Key`
        , Tags.Value       AS `Tags.Value`
    FROM ninjato.series;


SELECT 
    Datacenter
    , T 
    , ReqCount
    , round(ReqCount / TotalReqCount * 100, 2) AS "%"
FROM (
    SELECT 
        Tags.Value[indexOf(Tags.Key, 'datacenter')] AS Datacenter
        , DateTime AS T
        , SUM(ReqCount) AS ReqCount
    FROM metrics.example
    GROUP BY Datacenter, T 
    ORDER BY Datacenter, T 
) ALL INNER JOIN (
    SELECT
        DateTime AS T
        , SUM(ReqCount) AS TotalReqCount
    FROM metrics.example
    GROUP BY T
) USING (T)