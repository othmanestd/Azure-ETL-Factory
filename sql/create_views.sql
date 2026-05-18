-- Synapse Serverless SQL views for Power BI dashboards

-- Store daily performance
CREATE OR ALTER VIEW gold.vw_store_daily_performance AS
SELECT
    sale_date_key, store_id, store_name, region_clean AS region,
    country_clean AS country, gross_revenue, net_revenue,
    transaction_count, unique_customers, avg_basket_size,
    discount_rate, online_rate
FROM OPENROWSET(
    BULK 'https://retaildatalake.dfs.core.windows.net/gold/store_daily_performance/',
    FORMAT = 'DELTA'
) AS store_perf;

-- Category trends
CREATE OR ALTER VIEW gold.vw_category_trends AS
SELECT
    sale_month, category, total_revenue,
    units_sold, unique_buyers, avg_selling_price
FROM OPENROWSET(
    BULK 'https://retaildatalake.dfs.core.windows.net/gold/category_trends/',
    FORMAT = 'DELTA'
) AS cat_trends;

-- Regional revenue
CREATE OR ALTER VIEW gold.vw_regional_revenue AS
SELECT
    region_clean AS region, country_clean AS country,
    total_revenue, total_transactions, active_stores,
    unique_customers, avg_transaction_value
FROM OPENROWSET(
    BULK 'https://retaildatalake.dfs.core.windows.net/gold/regional_revenue/',
    FORMAT = 'DELTA'
) AS regional;

-- KPI summary for Power BI dashboard header
CREATE OR ALTER VIEW gold.vw_kpi_summary AS
SELECT
    SUM(gross_revenue) AS total_gross_revenue,
    SUM(net_revenue) AS total_net_revenue,
    SUM(transaction_count) AS total_transactions,
    SUM(unique_customers) AS total_unique_customers,
    AVG(avg_basket_size) AS avg_basket_size,
    AVG(discount_rate) AS avg_discount_rate,
    AVG(online_rate) AS avg_online_rate
FROM OPENROWSET(
    BULK 'https://retaildatalake.dfs.core.windows.net/gold/store_daily_performance/',
    FORMAT = 'DELTA'
) AS kpi;
