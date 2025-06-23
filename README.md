# Senior Data Engineer - Prep


SQL Practice

### 1. Write a SQL query to retrieve the 5 most recent flights (by departure_time) for each aircraft type, along with the flight number, departure time, and aircraft manufacturer.

```sql
flights
-----------
flight_id (PK)
flight_number
departure_time
aircraft_id (FK)
origin
destination

aircraft
-----------
aircraft_id (PK)
aircraft_type
manufacturer
manufacture_year
```



```sql
with ranked_flights as (

        select      flights.aircraft_id,
                    flights.flight_number,
                    flights.departure_time,
                    aircraft.aircraft_type,
                    aircraft.manufacturer
                    
        from        flights

        join        aircraft
                on flights.aircraft_id = aircraft.aircraft_id

        qualify row_number() over (
                        partition by aircraft_type
                        order by departure_time
                    ) <= 5

        
) 
select      flight_number, 
            aircraft_type, 
            manufacturer,
            departure_time
            
from        ranked_flights

``` 

“If this were standard SQL, I’d wrap the window function in a CTE and filter in a WHERE clause, but since Snowflake supports QUALIFY, it keeps the query more readable.”



### 2. You’re analyzing passenger traffic and want to examine week-over-week changes in flight volume. 

### Write a query that shows, for each origin-destination pair and week, the total number of flights that week, and the difference compared to the previous week.

```sql
daily_flight_counts
---------------------
flight_date (DATE)
origin
destination
flight_count

```

```sql
with weekly_aggregation as (

    select      concat_ws('_', origin, destination) as origin_destination
            ,   date_trunc('week', flight_date) as week_start__date
            ,   sum(flight_count) as flight_count

    from        daily_flight_counts 

    group by    origin_destination, week_start__date

),

weekly_aggregation_lagged as (

select      origin_destination
        ,   week_start__date
        ,   flight_count
        ,   lag(flight_count) over(
                partition by origin_destination
                order by week_start__date asc
        ) as flight_count__lag_1_wk

from    weekly_aggregation

)

select  origin_destination
    ,   week_start__date
    ,   flight_count
    ,   (flight_count - flight_count__lag_1_wk ) as flight_count__diff_1_wk

from    weekly_aggregation_lagged
```


and build a 7-day rolling average of flight volume, and do it in a way that's robust for production (e.g., dbt model).

```sql
daily_flight_counts
---------------------
flight_date (DATE)
origin
destination
flight_count (INT)
```

```sql
with daily_flight_counts as (
    select      flight_date
            ,   origin
            ,   destination
            ,   flight_count
    
    from {{ ref('stg_daily_flight_counts') }}
)


select      origin,
            destination,
            flight_count,
            avg(flight_count) over (
            partition by origin, destination
            order by flight_date
            rows between 6 preceding and current row
        ) as flight_count__7_day_RA

from    daily_flight_counts
```


### 3. You’re investigating whether bad weather at the destination airport correlates with delays. 
### You want to join each flight to the closest weather report at the destination airport that occurred within 1 hour before arrival. 
### For each flight, find the most recent weather report at the destination airport that occurred within 1 hour before arrival. 
### Write a SQL query to join each flight with the appropriate destination weather report, and return:

- flight_number, arrival_time, destination
- delay_minutes
- weather_condition, visibility_km

```sql
flights
-------------------------
flight_id (PK)
flight_number
departure_time
arrival_time
origin
destination
delay_minutes
```


```sql
weather_reports
-------------------------
airport_code
report_time
weather_condition
visibility_km
precipitation_mm
wind_speed_kph

```



```sql

with flights_and_conditions as (


    select      flights.flight_id
            ,   flights.arrival_time
            ,   flight_number
            ,   flights.origin
            ,   flights.destination
            ,   flights.delay_minutes
            ,   weather_reports.airport_code
            ,   weather_reports.report_time
            ,   weather_reports.weather_condition
            ,   weather_reports.visibility_km
            ,   row_number() over (
                partition by flights.flight_id
                order by weather_reports.report_time
            ) as flight_rank

    from        flights

    left join   weather_reports
             on flights.destination = weather_reports.airport_code
             and weather_reports.report_time between flights.arrival_time - INTERVAL '1 hour' and weather_reports.report_time
)


select          flight_id
            ,   flight_number
            ,   arrival_time
            ,   destination
            ,   delay_minutes
            ,   weather_condition
            ,   visibility_km

from            flights_and_conditions

where           flight_rank = 1

```