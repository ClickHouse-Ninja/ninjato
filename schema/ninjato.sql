CREATE DATABASE IF NOT EXISTS ninjato;
CREATE TABLE    IF NOT EXISTS ninjato.fields (
    ID UInt64 MATERIALIZED cityHash64(Key)
    , Key  String
    , Sign Int8 DEFAULT 1
) Engine CollapsingMergeTree(Sign)
PARTITION BY tuple()
ORDER     BY ID;

CREATE TABLE IF NOT EXISTS ninjato.series (
    DateTime  DateTime
    , Service String
    , Label   String
    , Value   Float64
    , Tags    Nested (
        Key String
        , Value String
    )
    , Fields Nested (
        Key String 
        , Value Float64
    )
) Engine Null;