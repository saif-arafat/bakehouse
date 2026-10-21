{# the body is a Databricks metric view YAML definition, not SQL: no columns to hold a contract to,
   and Fusion can't statically analyse it yet.
   Never put a ';' anywhere below, comments included: dbt splits statements on it before they reach
   Databricks, which cuts the YAML off mid-definition (PARSE_SYNTAX_ERROR at end of input) #}
{{ config(
    materialized='metric_view',
    contract={'enforced': false},
    static_analysis='off'
) }}

# one metric view per fact: a metric view has a single source and only many_to_one joins, so
# facts at another grain (reviews) get their own view (review_metrics) over the same conformed dimensions
version: 1.1
comment: >
  Sales KPIs over fct_sales (one row per transaction line), sliced by date, franchise, customer, product
  and payment method. Customer names and contact details are left out (PII). Sales that matched no
  customer or franchise sit on the '-1' unknown members.
source: "{{ ref('fct_sales') }}"

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
  - name: customer
    source: "{{ ref('dim_customer') }}"
    'on': source.customer_sk = customer.customer_sk
    joins:
      - name: customer_geography
        source: "{{ ref('dim_geography') }}"
        'on': customer.geography_sk = customer_geography.geography_sk
  - name: product
    source: "{{ ref('dim_product') }}"
    'on': source.product_sk = product.product_sk
  - name: payment
    source: "{{ ref('dim_payment_method') }}"
    'on': source.payment_method_sk = payment.payment_method_sk

fields:
  # when
  - name: sale_date
    expr: calendar.date_date
    display_name: Sale Date
    synonyms: [date, transaction date, order date]
  - name: sale_week
    expr: calendar.week_start_date
    display_name: Sale Week
    comment: Monday the sale's ISO week starts on.
    synonyms: [week]
  - name: sale_month
    expr: calendar.month_start_date
    display_name: Sale Month
    comment: First day of the sale's month.
    synonyms: [month]
  - name: day_of_week
    expr: calendar.day_name
    display_name: Day of Week
    synonyms: [weekday]
  - name: is_weekend
    expr: calendar.is_weekend
    display_name: Is Weekend
    synonyms: [weekend]
  - name: hour_of_day
    expr: hour(transaction_datetime)
    display_name: Hour of Day
    comment: Hour (0-23) of the ERP transaction timestamp.
    synonyms: [hour, time of day]

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
    comment: Standard country name of the franchise where the sale happened (not the customer's home country).
    synonyms: [country, store country]
  - name: franchise_continent
    expr: franchise.franchise_geography.country_continent
    display_name: Franchise Continent
    synonyms: [continent]

  # who (no names, emails, phones or addresses: those are PII-tagged in dim_customer)
  - name: customer_country
    expr: customer.customer_geography.country_name
    display_name: Customer Country
    comment: Standard country name of the buyer's home address (not where the sale happened).
    synonyms: [buyer country, home country]
  - name: customer_state
    expr: customer.customer_state
    display_name: Customer State
    comment: Full US state name of the buyer's address, null when the ERP value didn't match a state.
    synonyms: [buyer state, state]
  - name: customer_census_region
    expr: customer.customer_census_region
    display_name: Customer Census Region
    synonyms: [census region, us region]
  - name: customer_gender
    expr: customer.customer_gender
    display_name: Customer Gender
    synonyms: [gender]

  # what and how
  - name: product_name
    expr: product.product_name
    display_name: Product
    synonyms: [item, cookie]
  - name: payment_method
    expr: payment.payment_method_name
    display_name: Payment Method
    synonyms: [card type, payment type]

measures:
  - name: total_revenue
    expr: SUM(total_price)
    display_name: Total Revenue
    comment: Line revenue (quantity x unit price) as recorded by the ERP.
    synonyms: [revenue, sales, turnover]
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
  - name: units_sold
    expr: SUM(quantity)
    display_name: Units Sold
    synonyms: [quantity, items sold, volume]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: transaction_count
    expr: COUNT(1)
    display_name: Transactions
    comment: Number of sales. Each ERP transaction is a single product line.
    synonyms: [orders, number of sales, sales count]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: customer_count
    expr: "COUNT(DISTINCT customer_durable_sk) FILTER (WHERE customer_durable_sk <> '-1')"
    display_name: Customers
    comment: Distinct buying customers (each counted once across their history), leaving out sales on the unknown customer.
    synonyms: [unique customers, buyers, shoppers]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: active_franchise_count
    expr: "COUNT(DISTINCT franchise_sk) FILTER (WHERE franchise_sk <> '-1')"
    display_name: Active Franchises
    comment: Distinct franchises with at least one sale, leaving out the unknown franchise.
    synonyms: [stores selling, active stores, active branches]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: avg_transaction_value
    expr: SUM(total_price) / COUNT(1)
    display_name: Average Transaction Value
    synonyms: [AOV, average order value, average basket]
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
  - name: avg_unit_price
    expr: SUM(total_price) / SUM(quantity)
    display_name: Average Unit Price
    synonyms: [price per unit, average price]
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
