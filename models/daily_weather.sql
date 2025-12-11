{{ config(materialized='table') }}

WITH daily_weather1 as (

select 

date(time) as daily_weather,
weather,
temp,
humidity,
clouds,
pressure


 from {{ source('demo', 'weather') }}
 limit 10

),

daily_weather_agg as (

select
daily_weather,
weather,
round(AVG(temp),2) as avg_temp ,
round(AVG(humidity),2) as avg_humidity,
round(AVG(pressure),2) as avg_pressure,
round(AVG(clouds),2) as avg_clouds
from daily_weather1
group by daily_weather,weather

qualify ROW_NUMBER() OVER (PARTITION BY daily_weather ORDER BY count(weather) DESC) =1
)

select 
*
from daily_weather_agg