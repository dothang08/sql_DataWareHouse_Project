/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
  
CREATE VIEW gold.dim_customers AS
SELECT ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS Customer_key, -- Surrogate key
       ci.cst_id AS Customer_id,
       ci.cst_key AS Customer_number,
       ci.cst_firstname AS First_name,
       ci.cst_lastname AS Last_name,
       cl.cntry AS Country,
       ci.cst_marital_status AS Marital_Status,
       CASE
           WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr  -- CRM is the primary source for gender
           ELSE COALESCE(ca.gen, 'n/a')                -- Fallback to ERP data
       END AS Gender,
       ca.bdate AS Birthdate,
       ci.cst_create_date AS Create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca 
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS cl 
ON ci.cst_key = cl.cid;

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
  
CREATE VIEW gold.dim_products AS
SELECT ROW_NUMBER() OVER(ORDER BY prd_start_dt, pn.prd_key) AS Product_key, -- Surrogate key
       pn.prd_id AS Product_id,
       pn.prd_key AS Product_number,
       pn.prd_nm AS Product_name,
       pn.cat_id AS Category_id,
       pc.cat AS Category,
       pc.subcat AS SubCategory,
       pc.maintenance AS Maintenance,
       pn.prd_cost AS Cost,
       pn.prd_line AS Product_line,
       pn.prd_start_dt AS Start_date
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 AS pc 
ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; -- Filter out all historical data

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO
  
CREATE VIEW gold.fact_sales AS
SELECT sd.sls_ord_num AS Order_number,
       pr.Product_key,
       dc.Customer_key,
       sd.sls_order_dt AS Order_date,
       sd.sls_ship_dt AS Shipping_date,
       sd.sls_due_dt AS Due_date,
       sd.sls_sales AS Sales,
       sd.sls_quantity AS Quantity,
       sd.sls_price AS Price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS pr 
ON sd.sls_prd_key = pr.Product_number
LEFT JOIN gold.dim_customers AS dc 
ON sd.sls_cust_id = dc.Customer_id;

