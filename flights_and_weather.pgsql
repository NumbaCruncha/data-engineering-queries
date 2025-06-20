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
             and weather_reports.report_time between flights.arrival_time - INTERVAL '1 hour' 
             and weather_reports.report_time
)


select          flight_number
            ,   arrival_time
            ,   destination
            ,   delay_minutes
            ,   weather_condition
            ,   visibility_km


from            flights_and_conditions

-- where           flight_rank = 1
order by flight_number, arrival_time asc

