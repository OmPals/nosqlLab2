-- Databricks notebook source
drop table if exists diamonds;
create table diamonds using csv options (path "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header "true");

-- COMMAND ----------

describe diamonds

-- COMMAND ----------

drop table if exists diamonds;
create table diamonds using csv options (path "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header "true", inferSchema "true");

-- COMMAND ----------

describe diamonds;

-- COMMAND ----------

drop table if exists diamonds;
create table diamonds (_c0 String, carat double, cut String, color String, clarity String, depth double, dtable int, price double, x double, y double, z double) using csv options (path "dbfs:/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header "true");

-- COMMAND ----------

describe diamonds

-- COMMAND ----------

select * from diamonds limit 5

-- COMMAND ----------

select distinct clarity from diamonds

-- COMMAND ----------

select color, count(*) from diamonds group by color

-- COMMAND ----------

drop table if exists diamonds;

-- COMMAND ----------

drop table if exists iot_devices

-- COMMAND ----------

create table iot_devices using JSON options (path "dbfs:/databricks-datasets/iot/iot_devices.json")

-- COMMAND ----------

drop table if exists departuredelays;
create table departuredelays using csv options (path "/databricks-datasets/flights/departuredelays.csv", header "true")

-- COMMAND ----------

describe departuredelays

-- COMMAND ----------

select * from departuredelays limit 5

-- COMMAND ----------

drop table if exists airportcodes;
create table airportcodes using csv options (path "/databricks-datasets/flights/airport-codes-na.txt", header "true", delimiter "\t", inferSchema "true")

-- COMMAND ----------

describe airportcodes

-- COMMAND ----------

select * from airportcodes limit 5

-- COMMAND ----------

select substring (date, 0, 2) as month, avg(delay) as delay from departuredelays group by substring (date, 0, 2)

-- COMMAND ----------

select * from 
(select City, IATA, substring (date, 0, 2) as month, count(*) as arrivals_count 
from departuredelays join airportcodes on origin=IATA group by City, IATA, substring(date, 0, 2))

natural join (select City, IATA, substring(date, 0, 2) as month, count(*) as departures_count from departuredelays join airportcodes on destination = IATA group by City, IATA, substring (date, 0, 2))

-- COMMAND ----------

select City, IATA, count(*) as total_flights from ((select City, IATA, date
  from airportcodes join departuredelays on IATA=origin)
  union (select City, IATA, date from airportcodes join departuredelays on IATA=destination))
  where substring (date, 0, 2) = '03' group by City, IATA order by total_flights desc limit 1

-- COMMAND ----------


