#standardSQL
-- Weekly version, Not fix 0x.
WITH raw_data AS (
  SELECT 
    iosyearweek, 
--     d.year_week AS iosyearweek, 
    customer_id,
    CASE 
      WHEN  type = 'new_subscription' then 'C.Joins'
      WHEN  type = 'deactive' then 'B.Expired'
      ELSE 'A.Active' 
    END type_transformation,
    CASE 
      WHEN product_name LIKE '%1 Năm%' THEN '1.Gói 1 Năm'
      WHEN product_name LIKE '%1 Tháng%' THEN '3.Gói 1 Tháng'
--       WHEN product_name LIKE '%1 Tháng%' AND price = 0 THEN '3.Gói 1 Tháng Free'
      WHEN product_name LIKE '%3 Tháng%' THEN '2.Gói 3 Tháng'
      WHEN product_name LIKE '%6 Tháng%' THEN '4.Gói 6 Tháng'
--       WHEN product_name LIKE '%6 Tháng%' AND price = 0 THEN '4.1.Gói 6 Tháng Free'
      WHEN product_name is null THEN '5.Free trial'
--     ELSE '2.1.Gói 3 Tháng Free' 
    END product_name
  FROM `tiki-dwh.customer.bq_tikinow2h` AS b
--       LEFT JOIN `tiki-dwh.dwh.dim_date` AS d ON d.full_date = b.date 
  WHERE
    date >= 
    date_TRUNC(date_add(current_date("Asia/Ho_Chi_Minh"), interval -7 week), WEEK)
    and date <= current_date("Asia/Ho_Chi_Minh")
),
tikinow_by_package AS (
  SELECT 
    iosyearweek,
    type_transformation, 
    product_name,
    IF(type_transformation = 'B.Expired', - count(DISTINCT customer_id), count(DISTINCT customer_id)) qty
  FROM raw_data 
    GROUP BY iosyearweek, type_transformation, product_name 
    ORDER BY iosyearweek DESC, product_name
),
tiki_now_total AS (
  SELECT 
    iosyearweek,
    type_transformation, 
    "6.Total(1st day of week)" product_name,
    SUM(qty) qty
  FROM tikinow_by_package
    GROUP BY iosyearweek, type_transformation
)

SELECT * FROM tikinow_by_package
UNION ALL
SELECT * FROM tiki_now_total
