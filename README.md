# Transactions Analysis

## Overview

### a. Data Loading and Validation:

Load raw data from external storage (e.g., Azure Blobs, AWS S3) into the data warehouse using appropriate tools or frameworks (e.g., PySpark, AWS Glue).
Perform basic data validation checks:
Ensure columns are non-null and handle missing values appropriately.
Validate uniqueness of primary key columns to ensure data integrity.
Check and enforce data types to match the expected schema.
Establish foreign key constraints between fact and dimension tables:
Verify that foreign key columns in fact tables reference existing primary key values in dimension tables to maintain relational integrity.
### b. Staging Schema Creation:

Normalize the hierarchy table into separate dimension tables for each level (e.g., date, week, month) to reduce redundancy and improve data consistency.
Design a staging schema where the fact table is staged with foreign key relationships to the dimension tables:
Create dimension tables (dim_date, dim_week, dim_month) for each hierarchy level from the clnd hierarchy table.
Design the staged fact table (stg_fact_sales) with foreign key relationships to these dimension tables (dim_date_id, dim_week_id, dim_month_id).
### c. Creation of Refined Table (mview_weekly_sales):

Create a refined table (mview_weekly_sales) that aggregates sales metrics (sales_units, sales_dollars, discount_dollars) by specific dimensions (pos_site_id, sku_id, fsclwk_id, price_substate_id, type).
Utilize SQL aggregation functions to calculate total sales metrics grouped by the specified dimensions.
Implement the aggregation logic in a SQL view or table (mview_weekly_sales) for easy access and querying.

## ETL Pipeline

![ETL Pipeline](https://github.com/njska/skywalker/blob/master/ETL_Pipeline.drawio.png)

## Summary

Load and validate raw data from external storage, ensuring data quality through checks for non-null values, primary key uniqueness, and correct data types.
Normalize the hierarchy table into dimension tables and design a staging schema with foreign key relationships to prepare for efficient data processing and analysis.
Create a refined table that aggregates key sales metrics by specific dimensions, enabling insightful reporting and analysis of weekly sales data.

