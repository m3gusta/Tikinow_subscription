#standardSQL
SELECT DISTINCT 
     CASE 
      WHEN product_name LIKE '%1 Năm%' THEN '1.Gói 1 Năm'
      WHEN product_name LIKE '%1 Tháng%' THEN '3.Gói 1 Tháng'
--       WHEN product_name LIKE '%1 Tháng%' AND price = 0 THEN '3.Gói 1 Tháng Free'
      WHEN product_name LIKE '%3 Tháng%' THEN '2.Gói 3 Tháng'
      WHEN product_name LIKE '%6 Tháng%' THEN '4.Gói 6 Tháng'
--       WHEN product_name LIKE '%6 Tháng%' AND price = 0 THEN '4.1.Gói 6 Tháng Free'
      WHEN product_name is null THEN '5.Free trial'
--     ELSE '2.1.Gói 3 Tháng Free' 
    END product_name,
      iosyearweek, 
      platform, 
      count (*) count
      
FROM `tiki-dwh.customer.bq_tikinow2h`
-- LEFT JOIN `tiki-dwh.dwh.dim_date`  AS d ON d.full_date = date 
  WHERE type = 'new_subscription'
  AND date >=     date_TRUNC(date_add(current_date("Asia/Ho_Chi_Minh"), interval -7 week), WEEK)
  AND date <= current_date("Asia/Ho_Chi_Minh")
  GROUP BY 1,2,3
  ORDER BY iosyearweek
