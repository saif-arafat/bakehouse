{# the body is a Databricks metric view YAML definition, not SQL: no columns to hold a contract to,
   and Fusion can't statically analyse it yet.
   Never put a ';' anywhere below, comments included: dbt splits statements on it before they reach
   Databricks, which cuts the YAML off mid-definition (PARSE_SYNTAX_ERROR at end of input) #}
{{ config(
    materialized='metric_view',
    contract={'enforced': false},
    static_analysis='off'
) }}

# one metric view per fact: reviews are a different grain from sales, so they can't share sales_metrics.
# The franchise and date fields use the same names as sales_metrics so results line up across the two
version: 1.1
comment: >
  Review KPIs over fct_review (one row per web review), sliced by review date and franchise.
  Star ratings are extracted from the review text by ai_extract and are null when a review states none.
  Reviews that matched no franchise sit on the '-1' unknown member.
source: "{{ ref('fct_review') }}"

# fact -> dimension joins are many_to_one, the default
joins:
  - name: calendar
    source: "{{ ref('dim_date') }}"
    'on': source.date_sk = calendar.date_sk
  - name: franchise
    source: "{{ ref('dim_franchise') }}"
    'on': source.franchise_sk = franchise.franchise_sk
    joins:
      - name: franchise_geography
        source: "{{ ref('dim_geography') }}"
        'on': franchise.geography_sk = franchise_geography.geography_sk

fields:
  # when
  - name: review_date
    expr: calendar.date_date
    display_name: Review Date
    synonyms: [date]
  - name: review_week
    expr: calendar.week_start_date
    display_name: Review Week
    comment: Monday the review's ISO week starts on.
    synonyms: [week]
  - name: review_month
    expr: calendar.month_start_date
    display_name: Review Month
    comment: First day of the review's month.
    synonyms: [month]
  - name: day_of_week
    expr: calendar.day_name
    display_name: Day of Week
    synonyms: [weekday]

  # where
  - name: franchise_name
    expr: franchise.franchise_name
    display_name: Franchise
    synonyms: [store, branch, shop, location]
  - name: franchise_city
    expr: franchise.franchise_city
    display_name: Franchise City
    synonyms: [city, store city]
  - name: franchise_size
    expr: franchise.franchise_size
    display_name: Franchise Size
    synonyms: [store size]
  - name: franchise_country
    expr: franchise.franchise_geography.country_name
    display_name: Franchise Country
    synonyms: [country, store country]
  - name: franchise_continent
    expr: franchise.franchise_geography.country_continent
    display_name: Franchise Continent
    synonyms: [continent]

  # the review itself (the free text isn't a field: it isn't something to group by)
  - name: star_rating
    expr: star_rating
    display_name: Star Rating
    comment: Rating out of 5 stated in the review, null when it states none.
    synonyms: [stars, rating]
  - name: has_star_rating
    expr: has_star_rating
    display_name: Has Star Rating
    synonyms: [rated]

measures:
  - name: review_count
    expr: COUNT(1)
    display_name: Reviews
    synonyms: [number of reviews, review volume]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: rated_review_count
    expr: COUNT(star_rating)
    display_name: Rated Reviews
    comment: Reviews that state a star rating.
    synonyms: [reviews with a rating]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: avg_star_rating
    expr: AVG(star_rating)
    display_name: Average Star Rating
    comment: Mean rating over rated reviews only. Reviews without a rating are ignored, not counted as zero.
    synonyms: [average rating, avg stars, score]
    format:
      type: number
      decimal_places:
        type: exact
        places: 2
  - name: rated_review_share
    expr: COUNT(star_rating) / COUNT(1)
    display_name: Share of Reviews Rated
    comment: Fraction of reviews that state a star rating. Low values make the average rating less reliable.
    synonyms: [rating coverage]
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 1
  - name: avg_word_count
    expr: AVG(review_word_count)
    display_name: Average Review Length (Words)
    synonyms: [review length, words per review]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: reviewed_franchise_count
    expr: "COUNT(DISTINCT franchise_sk) FILTER (WHERE franchise_sk <> '-1')"
    display_name: Reviewed Franchises
    comment: Distinct franchises with at least one review, leaving out the unknown franchise.
    synonyms: [stores reviewed]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
