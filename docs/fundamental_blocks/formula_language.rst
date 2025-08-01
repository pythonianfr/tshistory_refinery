.. _formula_language:

Formula Language Reference
==========================

The Refinery formula language allows you to create computed time
series using a powerful Lisp-like functional language. Formulas are
evaluated dynamically and can reference other series, perform
calculations, and apply transformations.

Table of Contents
-----------------

- `Getting Started`_
- `Language Basics`_
- `Series Access and Options`_
- `Arithmetic and Mathematical Operations`_
- `Temporal Operations`_
- `Aggregation and Statistics`_
- `Data Processing and Filtering`_
- `Date and Time Functions`_
- `Advanced Series Discovery`_
- `Practical Examples`_
- `Best Practices`_

Getting Started
---------------

Your First Formula
~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Register a simple formula that adds two series
    tsa.register_formula(
        'total_sales',
        '(add (series "online_sales") (series "retail_sales"))'
    )

    # Use the formula series like any other series
    total = tsa.get('total_sales')

Basic Syntax Rules
~~~~~~~~~~~~~~~~~~

1. **Parentheses**: Every operation is wrapped in parentheses ``(function arg1 arg2 ...)``
2. **Quotes**: String literals use double quotes ``"series_name"``
3. **Keywords**: Optional parameters use ``#:keyword value`` syntax
4. **Nesting**: Functions can be nested ``(add (series "a") (mul (series "b") 2))``

Language Basics
---------------

Data Types
~~~~~~~~~~

.. code-block:: lisp

    ;; Numbers (integers and floats)
    42
    3.14159
    -100

    ;; Strings (series names, dates, etc.)
    "daily_temperature"
    "2024-01-01"
    "Europe/Paris"

    ;; Booleans
    #t  ;; true
    #f  ;; false

    ;; Dates (created with date function)
    (date "2024-01-01")
    (date "2024-01-01" #:tz "America/New_York")

    ;; Series (result of series operations)
    (series "stock_price")

Series Access and Options
--------------------------

Basic Series Access
~~~~~~~~~~~~~~~~~~~

.. code-block:: lisp

    ;; Simple series reference
    (series "temperature")

    ;; Series from different sources (if configured)
    (series "remote_data")  ;; Automatically resolves from available sources

Series Options
~~~~~~~~~~~~~~

Series options control how data is handled when combining with other series:

.. code-block:: lisp

    ;; Fill missing values
    (series "incomplete_data" #:fill "ffill")     ;; Forward fill
    (series "sparse_data" #:fill "bfill")         ;; Backward fill
    (series "default_zero" #:fill 0)              ;; Fill with constant value
    (series "mixed_fill" #:fill "ffill,bfill")    ;; Forward fill then backward fill

    ;; Preserve NaN values (useful for priority operations)
    (series "mask_data" #:keepnans #t)

    ;; Add weights for aggregation operations
    (series "important_station" #:weight 2.0)
    (series "minor_station" #:weight 0.5)

    ;; Combine multiple options
    (series "weather_data" #:fill "ffill" #:weight 1.5 #:keepnans #f)

Options Operator
~~~~~~~~~~~~~~~~

.. code-block:: lisp

    ;; Apply options to any series expression
    (options (series "raw_data") #:fill "ffill" #:weight 2)

    ;; Apply options to computed series
    (options (add (series "a") (series "b")) #:weight 1.5)

Arithmetic and Mathematical Operations
--------------------------------------

Scalar Operations
~~~~~~~~~~~~~~~~~

.. code-block:: lisp

    ;; Add constant to series
    (+ 100 (series "base_value"))                 ;; Add 100 to each point
    (+ (series "celsius") 273.15)                 ;; Convert Celsius to Kelvin

    ;; Multiply by constant
    (* (series "daily_value") 365)                ;; Annualize daily values
    (* -1 (series "debt"))                        ;; Flip sign

    ;; Divide by constant
    (/ (series "total") 1000)                     ;; Convert to thousands
    (/ (series "percentage") 100)                 ;; Convert percentage to decimal

    ;; Power operations
    (** (series "growth_rate") 12)                ;; Compound monthly to annual
    (** (series "distance") 2)                    ;; Square for area calculations

Series-to-Series Operations
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: lisp

    ;; Addition (variable number of arguments)
    (add (series "q1") (series "q2") (series "q3") (series "q4"))
    (add (series "domestic") (series "international"))

    ;; Multiplication
    (mul (series "price") (series "quantity"))              ;; Revenue calculation
    (mul (series "usd_amount") (series "usd_to_eur_rate"))  ;; Currency conversion

    ;; Division
    (div (series "revenue") (series "costs"))               ;; Profit margin
    (div (series "total_sales") (series "num_stores"))      ;; Sales per store

    ;; Subtraction (takes exactly 2 series)
    (sub (series "revenue") (series "costs"))               ;; Profit calculation
    (sub (series "target") (series "actual"))               ;; Variance analysis

Mathematical Functions
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: lisp

    ;; Absolute value
    (abs (series "temperature_diff"))                       ;; Remove negative signs
    (abs (sub (series "forecast") (series "actual")))       ;; Forecast error magnitude

    ;; Rounding
    (round (series "precise_values"))                       ;; Round to nearest integer
    (round (mul (series "percentage") 100))                 ;; Round percentages

**min** / **max** - Scalar min/max functions

.. code-block:: lisp

    (min 5.5 2.1 8.9)                                       ;; Returns 2.1
    (max 10 20 30 40)                                       ;; Returns 40
    (min (date "2024-01-01") (date "2024-06-01"))          ;; Earlier date
    (max 3.14 2.71 1.41)                                    ;; Returns 3.14

Trigonometric Functions
~~~~~~~~~~~~~~~~~~~~~~~

All trigonometric functions work with degrees (not radians):

.. code-block:: lisp

    ;; Basic trigonometric functions
    (trig.sin (series "angle_degrees"))                     ;; Sine
    (trig.cos (series "angle_degrees"))                     ;; Cosine
    (trig.tan (series "angle_degrees"))                     ;; Tangent

    ;; Inverse trigonometric functions (output in degrees)
    (trig.arcsin (series "ratio"))                          ;; Arcsine
    (trig.arccos (series "ratio"))                          ;; Arccosine
    (trig.arctan (series "slope"))                          ;; Arctangent

    ;; Two-argument arctangent (for vector angles)
    (trig.row-arctan2 (series "y_component") (series "x_component"))

    ;; Example: Convert wind components to direction
    (trig.row-arctan2 (series "wind_north") (series "wind_east"))

Temporal Operations
-------------------

Resampling (Downsampling)
~~~~~~~~~~~~~~~~~~~~~~~~~

Resampling reduces the frequency of data by aggregating values:

.. code-block:: lisp

    ;; Basic resampling with different aggregation methods
    (resample (series "hourly_temperature") "D")                    ;; Daily mean (default)
    (resample (series "minute_prices") "H" #:method "last")         ;; Hourly last value
    (resample (series "tick_volume") "15T" #:method "sum")          ;; 15-minute volume sum
    (resample (series "daily_rain") "M" #:method "sum")             ;; Monthly rainfall total

    ;; Advanced resampling
    (resample (series "irregular_data") "W" #:method "median")      ;; Weekly median
    (resample (series "high_freq") "Q" #:method "std")             ;; Quarterly volatility
    (resample (series "sensors") "Y" #:method "count")             ;; Annual data point count

    ;; Common frequency codes:
    ;; T/min - minutes, H - hours, D - days, W - weeks
    ;; M/ME - month end, Q/QE - quarter end, Y/YE - year end
    ;; Business frequencies: B - business days, BM - business month end

**freq** and **nfreq** - Frequency builders

.. code-block:: lisp

    (freq "H")           ;; Hourly frequency
    (freq "D")           ;; Daily frequency
    (nfreq 15 "T")       ;; 15-minute frequency
    (nfreq 3 "M")        ;; Quarterly frequency

    ;; Use with resample
    (resample (series "data") (freq "H"))              ;; To hourly
    (resample (series "data") (nfreq 15 "T"))          ;; To 15-minute
    (resample (series "data") (nfreq 2 "W"))           ;; To bi-weekly

Upsampling (Increasing Frequency)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Upsampling increases the frequency by interpolating or forward-filling:

.. code-block:: lisp

    ;; Convert monthly data to daily
    (upsample (series "monthly_budget") #:freq "D" #:origin-freq "M")

    ;; Convert annual data to quarterly with interpolation
    (upsample (series "yearly_target") #:freq "Q" #:origin-freq "Y" #:method "interpolate")

    ;; Convert weekly data to daily
    (upsample (series "weekly_sales") #:freq "D" #:origin-freq "W")

Time Shifting
~~~~~~~~~~~~~

.. code-block:: lisp

    ;; Basic shifting
    (time-shifted (series "data") #:hours 6)     ;; Shift forward 6 hours
    (time-shifted (series "data") #:days -1)     ;; Shift back 1 day

Three main use cases:

1. **Fix badly indexed series** - When timestamps are wrong by a consistent offset

   ;; Series was recorded in wrong timezone, shift by 5 hours
   (time-shifted (series "misindexed_data") #:hours 5)

2. **Business period alignment** - e.g., gas day (06:00 to 06:00)

   ;; Shift gas flow data so gas day aligns with calendar day
   (time-shifted (series "gas_flow") #:hours -6)
   ;; Now midnight values represent start of gas day

3. **"What if" scenarios** - Exploring hypothetical time shifts

   ;; What if maintenance was delayed by 2 days?
   (time-shifted (series "maintenance_schedule") #:days 2)

Rolling Operations
~~~~~~~~~~~~~~~~~~

Rolling operations compute statistics over a moving window:

.. code-block:: lisp

    ;; Moving averages
    (rolling (series "stock_price") 20)                            ;; 20-period moving average (default: mean)
    (rolling (series "temperature") 7 #:method "mean")             ;; 7-day average temperature
    (rolling (series "sales") 30 #:method "median")                ;; 30-day median sales

    ;; Rolling aggregations
    (rolling (series "daily_volume") 5 #:method "sum")             ;; 5-day volume sum
    (rolling (series "prices") 10 #:method "min")                  ;; 10-day minimum price
    (rolling (series "prices") 10 #:method "max")                  ;; 10-day maximum price

    ;; Rolling statistics
    (rolling (series "returns") 252 #:method "std")                ;; Annual volatility (252 trading days)
    (rolling (series "earnings") 4 #:method "count")               ;; 4-quarter data availability

    ;; Technical analysis examples
    (rolling (series "close") 50 #:method "mean")                  ;; 50-day moving average
    (rolling (series "high") 14 #:method "max")                    ;; 14-day highest high
    (rolling (series "low") 14 #:method "min")                     ;; 14-day lowest low

Cumulative Operations
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: lisp

    ;; Cumulative sum
    (cumsum (series "daily_sales"))                                ;; Running total of sales
    (cumsum (series "rainfall"))                                   ;; Total rainfall to date

    ;; Cumulative product
    (cumprod (series "daily_returns"))                             ;; Cumulative returns
    (cumprod (add 1 (series "growth_rates")))                      ;; Compound growth

Integration
~~~~~~~~~~~

The integration operator combines a stock series with a flow series:

.. code-block:: lisp

    ;; Basic integration
    (integration "stock-series-name" "flow-series-name")

    ;; Integration with fill (handles gaps in stock series)
    (integration "inventory-level" "daily-changes" #:fill #t)

    ;; Example: Tank level tracking
    (integration "tank-level-readings" "flow-meter-data")

This operator integrates the flow series from the last known stock value,
useful for tracking cumulative quantities where you have occasional
absolute readings (stock) and continuous change measurements (flow).

Block Staircase (Future Slicing)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The block-staircase operator extracts a consistent "slice of the future" from
historical forecast revisions:

.. code-block:: lisp

    ;; Extract 24-hour-ahead forecasts from daily runs
    (block-staircase "temperature-forecast"
                     #:revision_freq_days 1      ;; Daily forecast runs
                     #:revision_time_hours 6     ;; Run at 6am
                     #:maturity_offset_days 1)   ;; Extract 1-day-ahead slice

    ;; Extract 2-hour-ahead predictions from hourly model runs
    (block-staircase "demand-forecast"
                     #:revision_freq_hours 1     ;; Hourly updates
                     #:maturity_offset_hours 2)  ;; Extract 2-hour-ahead slice

What it does: For each point in the output series, it finds the forecast
that was made `maturity_offset` time ago. This reconstructs what you
"thought would happen" at a consistent horizon.

Use cases:
- Analyze forecast accuracy at specific horizons ("How good are our day-ahead forecasts?")
- Regulatory compliance ("What did we publish 24 hours in advance?")
- Compare model performance across different lead times

Aggregation and Statistics
--------------------------

Row-wise Operations
~~~~~~~~~~~~~~~~~~~

These operations work across multiple series at each time point:

.. code-block:: lisp

    ;; Row-wise mean (simple average)
    (row-mean (series "station_1") (series "station_2") (series "station_3"))

    ;; Weighted row-wise mean
    (row-mean (series "main_sensor" #:weight 3)
              (series "backup_sensor" #:weight 1))

    ;; Row-wise min/max
    (row-min (series "bid_price_1") (series "bid_price_2") (series "bid_price_3"))
    (row-max (series "server_cpu_1") (series "server_cpu_2"))

    ;; Row-wise standard deviation
    (std (series "measurement_1") (series "measurement_2") (series "measurement_3"))

    ;; Control NaN handling
    (row-mean (series "sensor_a") (series "sensor_b") #:skipna #f)  ;; Include NaNs

Day-of-Year Aggregation (Folded Series)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The doy-agg operator creates statistical profiles by day-of-year, useful for
"folded" visualizations where multiple years are overlaid on the same plot:

.. code-block:: lisp

    ;; Create median profile from 5 years of data
    (doy-agg (series "daily_temperature") 5 #:method "median")

    ;; Mean profile with stricter data requirements
    (doy-agg (series "energy_consumption") 4
             #:method "mean"
             #:valid_aggr_ratio 0.8)  ;; Require 80% of years to have data

    ;; Handle leap years for financial data
    (doy-agg (series "daily_revenue") 3
             #:leap_day_rule "ignore")  ;; Always 365 days

This creates the statistical summary line (median, mean, etc.) that you would
typically overlay on a folded plot showing individual years. The result has
one value per day-of-year, perfect for:

- Climate analysis (current year vs historical normal)
- Seasonal pattern detection
- Year-over-year comparisons on the same axis
- Building baseline expectations

Practical Aggregation Examples
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: lisp

    ;; Average temperature across weather stations
    (row-mean (series "station_north" #:weight 1)
              (series "station_south" #:weight 1)
              (series "station_city" #:weight 2))  ;; City station has double weight

    ;; Portfolio value calculation
    '(add (mul (series "stock_a_price") (series "stock_a_shares"))
          (mul (series "stock_b_price") (series "stock_b_shares"))
          (mul (series "stock_c_price") (series "stock_c_shares")))'

    ;; Risk calculation (worst case scenario)
    '(row-min (series "optimistic_forecast")
              (series "pessimistic_forecast")
              (series "realistic_forecast"))'

Data Processing and Filtering
-----------------------------

Conditional and Comparison Operations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: lisp

    ;; Comparison operators (return 0/1 series by default)
    (> (series "temperature") 25)                           ;; Greater than
    (>= (series "score") 70)                                ;; Greater or equal
    (< (series "price") 100)                                ;; Less than
    (<= (series "volume") 1000)                             ;; Less or equal
    (== (series "status") 1)                                ;; Equal to
    (<> (series "value") 0)                                 ;; Not equal to

    ;; With custom true/false values
    (> (series "temp") 25 #:true_value 100 #:false_value -100)
    (== (series "flag") 1 #:true_value "YES" #:false_value "NO")

    ;; Series-to-series comparisons
    (> (series "actual") (series "target"))

Clipping and Bounds
~~~~~~~~~~~~~~~~~~~

.. code-block:: lisp

    ;; Remove negative values
    (clip (series "temperature_anomaly") #:min 0)

    ;; Cap maximum values
    (clip (series "cpu_usage") #:max 100)

    ;; Set both bounds
    (clip (series "normalized_score") #:min 0 #:max 1)

    ;; Replace out-of-bounds values instead of removing them
    (clip (series "sensor_reading") #:min 0 #:max 1000 #:replacemin #t #:replacemax #t)

    ;; Outlier removal (keep values within 3 standard deviations)
    '(clip (series "measurements")
           #:min (sub (rolling (series "measurements") 100 #:method "mean")
                      (mul 3 (rolling (series "measurements") 100 #:method "std")))
           #:max (add (rolling (series "measurements") 100 #:method "mean")
                      (mul 3 (rolling (series "measurements") 100 #:method "std"))))'

Priority and Layering
~~~~~~~~~~~~~~~~~~~~~

The priority operator combines series by taking the first available value:

.. code-block:: lisp

    ;; Basic data fallback chain
    '(priority (series "real_time_data")
               (series "delayed_data")
               (series "historical_estimate"))'

    ;; Sensor data with backup
    '(priority (series "primary_sensor")
               (series "secondary_sensor")
               (constant 20 (date "2020-01-01") (date "2030-01-01") "H" (today)))'

    ;; Financial data with different frequencies
    '(priority (series "high_freq_prices")     ;; Prefer high-frequency data
               (series "daily_prices")         ;; Fall back to daily
               (series "weekly_estimates"))'   ;; Last resort

    ;; Combine actual and forecast data
    '(priority (series "actual_sales")         ;; Use actual when available
               (series "forecasted_sales"))'   ;; Fill gaps with forecast

Date-based Slicing
~~~~~~~~~~~~~~~~~~

.. code-block:: lisp

    ;; Slice from specific date
    (slice (series "historical_data") #:fromdate (date "2020-01-01"))

    ;; Slice to specific date
    (slice (series "forecast_data") #:todate (date "2025-12-31"))

    ;; Slice specific date range
    '(slice (series "training_data")
            #:fromdate (date "2018-01-01")
            #:todate (date "2022-12-31"))'

    ;; Dynamic slicing (last year of data)
    '(slice (series "recent_data")
            #:fromdate (shifted (today) #:days -365))'

Date-based Filtering with Cron Rules
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Filter data based on calendar patterns:

.. code-block:: lisp

    ;; Business hours only (9 AM to 5 PM, weekdays)
    '(date-filter (series "office_temperature")
                  (cronrule #:hour "9-17" #:day_week "1-5"))'

    ;; First day of each month
    '(date-filter (series "monthly_report_data")
                  (cronrule #:day_month "1"))'

    ;; Quarterly data (first day of Q1, Q2, Q3, Q4)
    '(date-filter (series "quarterly_metrics")
                  (cronrule #:month "1,4,7,10" #:day_month "1"))'

    ;; Weekend data only
    '(date-filter (series "leisure_activity")
                  (cronrule #:day_week "0,6"))'  # Sunday=0, Saturday=6

    ;; Peak hours in specific timezone
    '(date-filter (series "energy_consumption")
                  (cronrule #:hour "18-22")
                  #:tzone "Europe/Paris")'

    ;; Summer months only (June, July, August)
    '(date-filter (series "cooling_costs")
                  (cronrule #:month "6,7,8"))'

Revision Date Control
~~~~~~~~~~~~~~~~~~~~~

The asof operator fetches a series at a specific revision date:

.. code-block:: lisp

    ;; Get series as it was yesterday
    (asof (shifted (now) #:days -1) (series "volatile-series"))

    ;; Get series as of a specific date
    (asof (date "2024-01-01") (series "historical-data"))

    ;; Combine with other operations
    (add (asof (date "2023-12-31") (series "2023-values"))
         (series "2024-adjustments"))

Date and Time Functions
-----------------------

Current Date and Time
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: lisp

    ;; Current timestamp
    (now)                                   ;; Current UTC timestamp
    (now #:tz "America/New_York")           ;; Current time in NY timezone
    (now #:naive #t)                        ;; Naive timestamp (no timezone)

    ;; Today at midnight
    (today)                                 ;; Today at midnight UTC
    (today #:tz "Europe/London")            ;; Today at midnight London time
    (today #:naive #t)                      ;; Today naive (no timezone)

Date Creation
~~~~~~~~~~~~~

.. code-block:: lisp

    ;; Create specific dates
    (date "2024-01-01")                     ;; January 1, 2024 UTC
    (date "2024-07-15" #:tz "Asia/Tokyo")   ;; July 15, 2024 Tokyo time
    (date "2024-12-31")                     ;; December 31, 2024 UTC
    (date "2024-01-01" #:naive #t)          ;; January 1, 2024 naive (no tz)

    ;; Date with time
    (date "2024-06-15T14:30:00")            ;; June 15, 2024 at 2:30 PM UTC
    (date "2024-06-15T09:00:00" #:tz "Europe/Paris")  ;; 9 AM Paris time

    ;; Note: #:tz and #:naive are mutually exclusive

Date Arithmetic
~~~~~~~~~~~~~~~

.. code-block:: lisp

    ;; Add/subtract time periods
    (shifted (date "2024-01-01") #:days 30)              ;; 30 days later
    (shifted (date "2024-01-01") #:weeks -2)             ;; 2 weeks earlier
    (shifted (date "2024-01-01") #:months 6 #:days 15)   ;; 6 months 15 days later

    ;; Complex date arithmetic
    (shifted (today) #:years 1 #:months -3 #:days 5)     ;; 1 year, minus 3 months, plus 5 days
    (shifted (now) #:hours -24 #:minutes -30)            ;; 24.5 hours ago

    ;; Common patterns
    (shifted (today) #:days -7)                          ;; One week ago
    (shifted (today) #:days -365)                        ;; One year ago (approximately)
    (shifted (today) #:months 1)                         ;; One month from now

Month Boundary Functions
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: lisp

    ;; First day of month
    (start-of-month (date "2024-05-15"))     ;; Returns 2024-05-01 00:00:00
    (start-of-month (today))                 ;; First day of current month

    ;; Last day of month
    (end-of-month (date "2024-02-15"))       ;; Returns 2024-02-29 00:00:00 (leap year)
    (end-of-month (today))                   ;; Last day of current month

    ;; Monthly calculations
    (sub (end-of-month (today)) (start-of-month (today)))  ;; Days in current month

Timezone Operations
~~~~~~~~~~~~~~~~~~~

.. code-block:: lisp

    ;; Convert naive series to timezone-aware
    (tzaware (series "local_temperatures") "Europe/Berlin")

    ;; Convert timezone-aware to naive
    (naive (series "utc_timestamps") "America/Chicago")

    ;; Convert timestamp to different timezone
    (tzaware-stamp (date "2024-01-01T12:00:00") "Asia/Singapore")

    ;; Practical examples
    (tzaware (series "server_logs") "UTC")               ;; Standardize to UTC
    (naive (series "market_data") "America/New_York")    ;; Convert to NYSE local time

Holiday Calendar Series
~~~~~~~~~~~~~~~~~~~~~~~

Generate holiday indicator series (1.0 for holidays, 0.0 for regular days).
Takes a 2-letter country code (ISO 3166-1 alpha-2):

.. code-block:: lisp

    ;; French holidays (tzaware by default)
    (holidays "fr" (date "2025-01-01") (date "2025-12-31"))

    ;; US holidays as naive timestamps
    (holidays "us" (date "2025-01-01") (date "2025-12-31") #:naive #t)

    ;; German holidays for current year
    (holidays "de" (start-of-month (today)) (end-of-month (shifted (today) #:months 12)))

Special Series Generation
-------------------------

**constant** - Generate a constant-valued time series

.. code-block:: lisp

    (constant 42.5 (date "2020-01-01") (date "2025-12-31") "D" (today))
    ;; Creates daily series with value 42.5 from 2020 to 2025

    ;; With timezone
    (constant 100
              (date "2020-01-01" #:tz "Europe/Paris")
              (date "2025-12-31" #:tz "Europe/Paris")
              "H"
              (today #:tz "Europe/Paris"))

Advanced Series Discovery
--------------------------

Dynamic Series Selection
~~~~~~~~~~~~~~~~~~~~~~~~

Instead of hardcoding series names, you can dynamically find and load series:

.. code-block:: lisp

    ;; Find and load all temperature series
    (findseries (by.metaitem "sensor_type" "temperature"))

    ;; Find series with fill options
    (findseries (by.name "daily") #:fill "ffill")

    ;; Get just the names (for inspection)
    (findnames (by.metaitem "region" "europe"))

    ;; Use in calculations
    (row-mean (findseries (by.metaitem "station_type" "weather")))  ;; Average all weather stations

    ;; Filter by timezone awareness (naive defaults to #f)
    (findseries (by.metaitem "type" "price"))              ;; All series (default: naive=#f)
    (findseries (by.metaitem "type" "price") #:naive #f)   ;; Only tzaware series (explicit)
    (findseries (by.metaitem "type" "daily") #:naive #t)   ;; Only naive series

Basket-based Operations
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: lisp

    ;; Use predefined baskets
    (findseries (by.basket "energy_meters"))
    (add (findseries (by.basket "revenue_streams")))

    ;; Combine with other operations
    (rolling (row-mean (findseries (by.basket "temperature_sensors"))) 7)
