#standardSQL
-- Weekly data, not edit 0x
WITH 
new_subscription AS (
    SELECT DISTINCT 
        iosyearweek ,
        product_id,
        order_id,
        order_code,
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
        date,
        customer_id, 
        rank () over (partition by customer_id order by iosyearweek) rank
      FROM `tiki-dwh.customer.bq_tikinow2h` AS b 
--       LEFT JOIN `tiki-dwh.dwh.dim_date` AS d ON d.full_date = b.date 
      WHERE type = 'new_subscription'
      ORDER BY customer_id, rank
),
auto_renewed_subscription AS (
  SELECT DISTINCT
     order_code,
     'Auto-renew subscription' Auto_renewal_subscription
  FROM `tiki-dwh.ecom.subscription_purchase` 
  WHERE status = 1
),
data AS(
  SELECT distinct
      a.*, 
      CASE 
        WHEN a.rank = 1 then 'New subscriber' 
        ELSE 'Customers renew subscription' end customer_type,
      b.Auto_renewal_subscription 
  FROM new_subscription a 
            LEFT JOIN auto_renewed_subscription b ON b.order_code = a.order_code
  WHERE 
    date >= date_TRUNC(date_add(current_date("Asia/Ho_Chi_Minh"), interval -7 WEEK), WEEK)
    and date <= current_date("Asia/Ho_Chi_Minh")
)
     
-- select * from new_subscription     
     
SELECT DISTINCT 
    iosyearweek,
    product_name,
--     IFNULL(Auto_renewal_subscription, customer_type) Customer_type,
    case 
    when customer_type = 'New subscriber' and Auto_renewal_subscription = 'Auto-renew subscription' then 'New subscriber - CC'
    when customer_type = 'New subscriber' and Auto_renewal_subscription is null then 'New subscriber - NCC'
    when customer_type = 'Customers renew subscription' and Auto_renewal_subscription = 'Auto-renew subscription'  then 'Return Customer - CC'
    else 'Return Customer - Other' end as Customer_type, 
    COUNT (*) New_subscription
FROM data
  GROUP BY iosyearweek, product_name, Customer_type
  ORDER BY iosyearweek DESC, Customer_type DESC

-- 1013112 Dịch Vụ TikiNOW (Gói 1 Năm)	
-- 1012902 Dịch Vụ TikiNOW (Gói 1 Tháng)		
-- 1013062 Dịch Vụ TikiNOW (Gói 3 Tháng)	
-- 13828475	Dịch Vụ TikiNOW (Gói 6 Tháng)	


-- with raw as(
-- select 
--   product_id,
--   count(*) as num
-- from `ecom.customer_subscription_summary` 
-- group by 1
-- -- order by num desc
-- )

-- select 
--   t1.*, t2.product_name 
-- from raw t1
-- left join `dwh.dim_product` t2 using (product_id)
-- order by num desc
