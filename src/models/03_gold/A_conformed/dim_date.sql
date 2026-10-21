-- date_spine renders a full `with ... select` query, so it must be the CTE body, not a FROM target
WITH spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2000-01-01' as date)",
        end_date="cast('2060-01-01' as date)"
    ) }}
),

base AS (
    SELECT cast(date_day AS date) AS d
    FROM spine
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['d']) }}    AS date_sk,
    cast(date_format(d, 'yyyyMMdd') AS int)          AS date_int,       -- 20261017
    d                                                AS date_date,

    -- year
    year(d)                                          AS date_year,
    trunc(d, 'YEAR')                                 AS year_start_date,
    make_date(year(d), 12, 31)                       AS year_end_date,
    dayofyear(d)                                     AS date_day_of_year,
    day(last_day(make_date(year(d), 2, 1))) = 29     AS is_leap_year,

    -- quarter
    quarter(d)                                       AS date_quarter,
    concat('Q', quarter(d))                          AS quarter_name,      -- Q4
    concat(year(d), '-Q', quarter(d))                AS year_quarter,      -- 2026-Q4
    trunc(d, 'QUARTER')                              AS quarter_start_date,
    date_add(add_months(trunc(d, 'QUARTER'), 3), -1) AS quarter_end_date,

    -- month
    month(d)                                         AS date_month,
    date_format(d, 'MMMM')                           AS month_name,        -- October
    date_format(d, 'MMM')                            AS month_name_short,  -- Oct
    date_format(d, 'yyyy-MM')                        AS year_month,        -- 2026-10
    trunc(d, 'MONTH')                                AS month_start_date,
    last_day(d)                                      AS month_end_date,
    day(last_day(d))                                 AS days_in_month,
    d = trunc(d, 'MONTH')                            AS is_month_start,
    d = last_day(d)                                  AS is_month_end,

    -- week (ISO: weeks start Monday, week 1 contains the first Thursday)
    weekofyear(d)                                    AS iso_week_of_year,
    extract(yearofweek FROM d)                       AS iso_year,          -- differs from date_year around Jan 1
    concat(
        extract(yearofweek FROM d), '-W',
        lpad(weekofyear(d), 2, '0')
    )                                                AS iso_year_week,     -- 2026-W42
    trunc(d, 'WEEK')                                 AS week_start_date,   -- Monday
    date_add(trunc(d, 'WEEK'), 6)                    AS week_end_date,     -- Sunday

    -- day
    day(d)                                           AS date_day_of_month,
    dayofweek(d)                                     AS date_day_of_week,  -- 1 = Sunday ... 7 = Saturday
    extract(dayofweek_iso FROM d)                    AS iso_day_of_week,   -- 1 = Monday ... 7 = Sunday
    date_format(d, 'EEEE')                           AS day_name,          -- Saturday
    date_format(d, 'EEE')                            AS day_name_short,    -- Sat
    dayofweek(d) IN (1, 7)                           AS is_weekend,
    dayofweek(d) NOT IN (1, 7)                       AS is_weekday,

    -- handy comparisons
    date_add(d, -1)                                  AS prior_day_date,
    add_months(d, -12)                               AS same_day_prior_year_date,

    current_timestamp()                              AS md_loaded_at_ts
FROM base