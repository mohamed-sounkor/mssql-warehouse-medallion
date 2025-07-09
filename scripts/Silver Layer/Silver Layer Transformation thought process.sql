-- documenting the crm_cust_info data transformation thought process
/* 
- customer id and customer key will remain the same
- first name and last name will be trimmed to remove any leading or trailing spaces
- marital status will be normalized to "Married", "Single", or "N/A" for empty and null values
- gender will be normalized to "Male", "Female", or "N/A" for empty and null values
- create date will be left as it is.
*/

-- documenting the crm_prd_info data transformation thought process
/*
- product id will remain the same since the count of the distinct product id is the same as the count of the whole table, meaning it's unique and not duplicated
- raw product key will remain the same but the table will be enriched by 
  category id and product key which are derived from the original product key
- category id will have the "-" replaced with "_" to match the naming convention in the erp_px_cat_g1v2 table
- product name will be trimmed to remove any leading or trailing spaces
- product cost will be cleaned from negative or null values
- product line will be normalized to "Mountain", "Road", "Touring", or "Other Sales"
- Product start date will br cast as Date to omit the time part
- Product end date will end the day before the next sart date to avoid date overlaps
*/

-- documenting the crm_sales_details data transformation thought process
/*
- order number, product key, and customer id will be left as they are
- order date will be cast as Date and cleaned by setting any incomplete date to null
- same for ship and due dates although the current data does not have any incomplete dates, but for consistency and future data.
- sales will be cleaned from negative or null values
- quantity will be cleaned from negative or null values
  sales will be recalculated as quantity * sales price if the numbers didn't add up
- sales quantity will be cleaned from negative or null values
- sales price will also be cleaned and recalculated as sales / quantity if the numbers didn't add up
*/

-- documenting the erp_cust_az12 data transformation thought process
/*
- The cid have the "NAS" prefix trimmed to match the customer key in the crm_cust_info table
- birthdates will be cleaned from future dates
- gender values will be normalized to either "Male" or "Female" or "N/A" for empty and null values
*/

-- documenting the erp_px_cat_g1v2 data transformation thought process
/* 
- It's all cleaned and normalized data, so no transformation is needed
*/

-- documenting the erp_loc_a101 data transformation thought process
/* 
- cid will have the "-" removed to match the customer key in the crm_cust_info table
- location will be trimmed and normalized, USA and US and United States will be normalized to United states, 
  DE and Germany will be normalizzed to Germany, null and emoty values will be handled as "N/A"
*/
SELECT * FROM bronze.crm_cust_info
SELECT * FROM bronze.crm_prd_info
SELECT * FROM bronze.crm_sales_details
SELECT * FROM bronze.erp_cust_az12
SELECT * FROM bronze.erp_px_cat_g1v2
SELECT * FROM bronze.erp_loc_a101

SELECT 
  DISTINCT LEN(cid),
  COUNT(*)
 FROM bronze.erp_cust_az12
 GROUP BY LEN(cid)

SELECT DISTINCT 
  LEN(cst_key),
  COUNT(*)
 FROM bronze.crm_cust_info
 GROUP BY LEN(cst_key)

SELECT cst_key FROM bronze.crm_cust_info
WHERE LEN(cst_key) <> 10

SELECT 
  DISTINCT gen,
  COUNT(*)
 FROM bronze.erp_cust_az12
 GROUP BY gen

 SELECT DISTINCT 
  LEN(cid),
  COUNT(*)
 FROM bronze.erp_loc_a101
 GROUP BY LEN(cid)

 SELECT DISTINCT cntry FROM bronze.erp_loc_a101