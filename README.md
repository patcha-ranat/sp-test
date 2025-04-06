# sp-test

*Patcharanat P.*

*There's notebook workspace at: [03_Python script_Patcharanat.ipynb](03_Python%20script_Patcharanat.ipynb), if output observation is required*

## Table of Contents

- [Project Installation](#project-installation)
- [Data initialization](#data-initialization)
- [01-SQL](#01-sql)
    - [Q1.1: Simple SQL Aggregation](#q11-simple-sql-aggregation)
    - [Q1.2: First Time Purchase Count in a Month](#q12-first-time-purchase-count-in-a-month)
    - [Q1.3: SQL Category Item Contribute Rate 50%](#q13-sql-category-item-contribute-rate-50)
    - [Q1.4: Skipped](#q14-skipped)
- [02-Excel](#02-excel)
- [03-Pyspark](#03-pyspark)
    - [Q3.1: Pyspark Aggregation - Level Item or Region](#q31-pyspark-aggregation---level-item-or-region)
    - [Q3.2: Competitiveness Priority](#q32-competitiveness-priority)
    - [Q3.3: Count Item for Highest Model Ordered Tops 30%](#q33-count-item-for-highest-model-ordered-tops-30)
    - [Q3.4: Dashboard and Presentation](#q34-dashboard-and-presentation)
    - [Q3.5: Extra Tops 30% Flag](#q35-extra-tops-30-flag)
- [Reference](#reference)

## Project Installation
```bash
python -m venv venv

source venv/Scripts/activate

make install
```


## Data Initialization

To read the given excel data with `pyspark`, there's some overhead of dependencies management by managing **jars** file.

To simplify this step, I use pandas to extract data from excel with more minimal dependencies such as `pandas`, and `openpyxl` and load it as simple csv file for futher processing with pyspark.

## 01-SQL

### Q1.1: Simple SQL Aggregation

```sql
SELECT
  candidate_id,
  COUNT(*) AS count_duplicated
FROM
  `shopee.test_candidate_03`
GROUP BY
  candidate_id
HAVING
  count_duplicated > 1
ORDER BY
  candidate_id
```

### Q1.2: First Time Purchase Count in a Month

1. Alternative 1, in case we need just available data to be shown.
```sql
WITH
TRANSACTIONS AS (
    SELECT
        buyer_id,
        DATE(create_datetime) AS dt,
        cluster_orders_item
    FROM 
        order_item
    WHERE
        DATE(create_datetime) BETWEEN "2021-04-01" AND "2021-04-30"
),

FIRST_TIME_BUY AS (
    SELECT
        buyer_id,
        DATE(result_first_purchase_time) AS dt
    FROM
        first_purchase
    WHERE
        DATE(result_first_purchase_time) BETWEEN "2021-04-01" AND "2021-04-30"
    
),

FILTERED_FIRST_TIME_BUY AS (
    SELECT
        FIRST_TIME_BUY.dt AS dt,
        FIRST_TIME_BUY.buyer_id AS buyer_id
    FROM
        FIRST_TIME_BUY
    LEFT JOIN
        TRANSACTIONS
        ON
            FIRST_TIME_BUY.buyer_id = TRANSACTIONS.buyer_id
    WHERE
        cluster_orders_item = "Fashion"
)

SELECT
    dt AS create_date,
    COUNT(*) AS num_of_first_purchase_buyers
FROM
   FILTERED_FIRST_TIME_BUY
GROUP BY
    dt
ORDER BY
    dt ASC
```

2. Alternative 2, in case we need all date to be specified explicitly.

```sql
WITH
TRANSACTIONS AS (
    SELECT
        buyer_id,
        DATE(create_datetime) AS dt,
        cluster_orders_item
    FROM 
        order_item
    WHERE
        DATE(create_datetime) BETWEEN "2021-04-01" AND "2021-04-30"
),

FIRST_TIME_BUY AS (
    SELECT
        buyer_id,
        DATE(result_first_purchase_time) AS dt
    FROM
        first_purchase
    WHERE
        DATE(result_first_purchase_time) BETWEEN "2021-04-01" AND "2021-04-30"
    
),

FILTERED_FIRST_TIME_BUY AS (
    SELECT
        FIRST_TIME_BUY.dt AS dt,
        FIRST_TIME_BUY.buyer_id AS buyer_id
    FROM
        FIRST_TIME_BUY
    LEFT JOIN
        TRANSACTIONS
        ON
            FIRST_TIME_BUY.buyer_id = TRANSACTIONS.buyer_id
    WHERE
        cluster_orders_item = "Fashion"
),

FINAL_FIRST_TIME_BUY AS (
    SELECT
        dt,
        COUNT(*) AS num_of_first_purchase_buyers
    FROM
        FILTERED_FIRST_TIME_BUY
    GROUP BY
        dt
),

DATE_RANGE AS (
    SELECT
        EXPLODE(SEQUENCE(TO_DATE('2021-04-01'), 
        TO_DATE('2021-04-30'), INTERVAL 1 DAY)) AS create_date
)

SELECT
    create_date,
    COALESCE(num_of_first_purchase_buyers, 0) AS num_of_first_purchase_buyers
FROM
    DATE_RANGE
LEFT JOIN
    FINAL_FIRST_TIME_BUY
    ON
    DATE_RANGE.create_date = FINAL_FIRST_TIME_BUY.dt
ORDER BY
    create_date ASC
```

### Q1.3: SQL Category Item Contribute Rate 50% 
```sql
WITH
TOTAL_ORDER AS (
    SELECT 
        partial_order_item_level.order_id,
        partial_order_item_level.item_id,
        DATE(partial_order_item_level.create_timestamp) AS create_date,
        item_category.cluster
    FROM
        partial_order_item_level
    LEFT JOIN
        item_category
        ON
            partial_order_item_level.item_id = item_category.item_id
    WHERE
        DATE(partial_order_item_level.create_timestamp) BETWEEN "2024-07-01" AND "2024-07-31"
),

CLUSTER_ITEM_COUNT AS (
    SELECT
        cluster,
        item_id,
        COUNT(DISTINCT order_id) AS total_item_per_cluster
    FROM
        TOTAL_ORDER
    GROUP BY
        cluster, item_id
),

CLUSTER_ORDER_COUNT AS (
    SELECT
        cluster,
        COUNT(DISTINCT order_id) AS total_order_per_cluster
    FROM
        TOTAL_ORDER
    GROUP BY
        cluster
),

CALCULATE_CONTRIBUTION AS (
    SELECT
        CLUSTER_ITEM_COUNT.cluster,
        item_id,
        CLUSTER_ITEM_COUNT.total_item_per_cluster,
        CLUSTER_ORDER_COUNT.total_order_per_cluster,
        CLUSTER_ITEM_COUNT.total_item_per_cluster / CLUSTER_ORDER_COUNT.total_order_per_cluster AS contribution_rate
    FROM
        CLUSTER_ITEM_COUNT
    LEFT JOIN
        CLUSTER_ORDER_COUNT
        ON
            CLUSTER_ITEM_COUNT.cluster = CLUSTER_ORDER_COUNT.cluster 
),

RANK_CONTRIBUTION AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY cluster ORDER BY contribution_rate DESC) AS contribution_rank
    FROM
        CALCULATE_CONTRIBUTION
)

SELECT
    item_id,
    cluster
FROM
    RANK_CONTRIBUTION
WHERE
    contribution_rate >= 0.5
ORDER BY
    cluster, contribution_rank DESC
```

### Q1.4: Skipped

*skipped*

## 02-EXCEL

*Answers provided in the excel file*

## 03-Pyspark

*There's jupyter notebook workspace at: [03_Python script_Patcharanat.ipynb](03_Python%20script_Patcharanat.ipynb), if output observation is required*

### Q3.1: Pyspark Aggregation - Level Item or Region

```bash
python ./spark_code/q1.py
# check result from terminal
```

### Q3.2: Competitiveness Priority

```bash
python ./spark_code/q2.py
# check result from terminal
```

### Q3.3: Count Item for Highest Model Ordered Tops 30%

```bash
python ./spark_code/q3.py
# check result from terminal
```

### Q3.4: Dashboard and Presentation

*Answer provided through GG slide*

### Q3.5: Extra Tops 30% Flag

```bash
python ./spark_code/q5.py
# check result from terminal
```

## Reference

- [Trying to use pyspark to read xlsx dataset (jars file approach) - Stack Overflow](https://stackoverflow.com/questions/56426069/how-to-read-xlsx-or-xls-files-as-spark-dataframe)
- [Registering temp Table - Spark Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.registerTempTable.html)