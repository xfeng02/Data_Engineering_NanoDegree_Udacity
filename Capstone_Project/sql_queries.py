import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_covid_table_drop = "drop table if exists staging_covid"
staging_temp_table_drop = "drop table if exists staging_temp"
staging_pop_table_drop ="drop table if exists staging_pop"
fact_covid_table_drop = "drop table if exists fact_covid"
dim_location_table_drop = "drop table if exists dim_location"
dim_time_table_drop = "drop table if exists dim_time"

# CREATE TABLES

staging_covid_table_create= ("""
create table if not exists staging_covid (
FIPS varchar,
Admin2 varchar,

State varchar,
Country varchar,
Last_update TIMESTAMP,

Latitude numeric,
Longitude numeric,
Confirmed numeric,
Deaths numeric,
Recovered numeric,
Date DATE, 
Year integer,
Month integer


)


""")

staging_temp_table_create = ("""
create table if not exists staging_temp (
Country varchar,
State varchar,
AverageTemperature numeric

)


""")

staging_pop_table_create = ("""
create table if not exists staging_pop (
Country varchar,
State varchar,
Population numeric

)


""")



fact_covid_table_create = ("""
create table if not exists fact_covid (
    covid_id INTEGER IDENTITY (1, 1) primary key distkey,
    Last_update timestamp NOT NULL sortkey ,
    location_id integer,  
    Confirmed numeric,
    Deaths numeric,
    Recovered numeric,
    Date DATE NOT NULL, 
    Year integer NOT NULL,
    Month integer NOT NULL
)

""")

dim_location_table_create = ("""
create table if not exists dim_location (
    location_id INTEGER IDENTITY (1, 1) primary key sortkey,

    State varchar ,
    Country varchar NOT NULL,
    AverageTemperature numeric,
    Population numeric

)
diststyle all;
""")


dim_time_table_create = ("""
create table if not exists dim_time (
    Last_update timestamp NOT NULL primary key sortkey,
    day int NOT NULL,
    week int NOT NULL,
    month int NOT NULL,
    year int NOT NULL, 
    weekday int
)
diststyle all;
""")


# STAGING TABLES

staging_covid_copy = ("""

COPY staging_covid FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION '{}'
FORMAT AS PARQUET

""").format(config.get('S3','COVID_DATA'), config.get('IAM_ROLE','ARN') ,config.get('REGION','REGION'))

staging_temp_copy = ("""

COPY staging_temp FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION '{}'
FORMAT AS PARQUET

""").format(config.get('S3','TEMP_DATA'), config.get('IAM_ROLE','ARN'),config.get('REGION','REGION'))

staging_pop_copy = ("""

COPY staging_pop FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION '{}'
FORMAT AS PARQUET

""").format(config.get('S3','POP_DATA'), config.get('IAM_ROLE','ARN'),config.get('REGION','REGION'))

# FINAL TABLES

dim_location_table_insert =("""

insert into dim_location (
 
  
    State,
    Country,
    AverageTemperature,
    Population

) select distinct 
  
    t1.State,
    t1.Country,
    t2.AverageTemperature,
    t3.Population
    from staging_covid t1 left join staging_temp t2 on t1.State=t2.State and t1.Country=t2.Country
        left join staging_pop t3 on t1.State=t3.State and t1.Country=t3.Country
    order by t1.Country, t1.State

""")


fact_covid_table_insert = ("""

insert into fact_covid (
   
    Last_update,
    location_id,    
    Confirmed,
    Deaths,
    Recovered,
    Date, 
    Year,
    Month ) 
    
    select distinct
        c.Last_update,
        l.location_id,
        sum(c.Confirmed) as Confirmed,
        sum(c.Deaths) as Deaths,
        sum(c.Recovered) as Recovered,
        c.Date, 
        c.Year,
        c.Month     

    from staging_covid c 
    
    left join dim_location l on c.State=l.State and c.Country=l.Country
    group by c.Last_update, l.location_id, c.Date, c.Year, c.Month 
    order by c.Last_update, l.location_id
""")

dim_time_table_insert = ("""
insert into dim_time ( 
    Last_update timestamp NOT NULL primary key sortkey,
    day,
    week,
    month,
    year, 
    weekday) 

    select distinct 
        c.Last_update,
        EXTRACT (DAY FROM c.Last_update) as day,
        EXTRACT (WEEK FROM c.Last_update) as week, 
        EXTRACT (MONTH FROM c.Last_update) as month,
        EXTRACT (YEAR FROM c.Last_update) as year,
        EXTRACT (WEEKDAY FROM c.Last_update) as weekday
    from staging_covid c
    order by c.Last_update



""")

# QUERY LISTS

create_table_queries = [staging_covid_table_create, staging_temp_table_create,staging_pop_table_create, fact_covid_table_create,  dim_location_table_create, dim_time_table_create]
drop_table_queries = [staging_covid_table_drop, staging_temp_table_drop,  staging_pop_table_drop, fact_covid_table_drop, dim_location_table_drop, dim_time_table_drop]
copy_table_queries = [staging_covid_copy, staging_temp_copy, staging_pop_copy]
insert_table_queries = [dim_location_table_insert, fact_covid_table_insert, dim_time_table_insert]
