with raw_2h as
(select 
    date,
    CASE 
      WHEN product_name LIKE '%1 Năm%' then '12'
      WHEN product_name LIKE '%1 Tháng%' then '1'
      WHEN product_name LIKE '%3 Tháng%' then '3'
      WHEN product_name LIKE '%6 Tháng%' then '6'
      ELSE 'FT' END product_type,
      if(min(if(type = 'new_subscription',created_date, null )) OVER (PARTITION BY customer_id) = created_date    
      and date_Add(created_date,interval 30 day) = date
      ,1,0) fisrt_month,
      
 if(concat(cast(EXTRACT(ISOYEAR FROM current_date("Asia/Ho_Chi_Minh")) as STRING),'-',
    cast(EXTRACT(ISOWEEK FROM current_date("Asia/Ho_Chi_Minh")) as STRING)) = iosyearweek,
      current_date("Asia/Ho_Chi_Minh"),
      date_sub(date_TRUNC(date, ISOWEEK),INTERVAL  1 day)) end_date ,
      
 if(concat(cast(EXTRACT(ISOYEAR FROM current_date("Asia/Ho_Chi_Minh")) as STRING),'-',
    cast(EXTRACT(ISOWEEK FROM current_date("Asia/Ho_Chi_Minh")) as STRING)) = iosyearweek,
      current_date("Asia/Ho_Chi_Minh"),
      date_sub(date_TRUNC(date, ISOWEEK) ,INTERVAL  30 day)) start_date,
      
cus_backend_id customer_id,
if(type = 'deactive',0,1) is_active
from `tiki-dwh.customer.bq_tikinow2h` 
where  
date >=
date_TRUNC(date_add(current_date("Asia/Ho_Chi_Minh"), interval -7 WEEK), ISOWEEK)
and 
date <
date_TRUNC(current_date("Asia/Ho_Chi_Minh"), ISOWEEK)
-- date_add(date_TRUNC(current_date("Asia/Ho_Chi_Minh"),WEEK),interval 7 day)
),

total_active_all as(
SELECT iosyearweek, product_name,
    case when product_name = '2.Gói 3 Tháng' then '3' 
      when product_name = '3.Gói 1 Tháng' then '1' 
      when product_name = '4.Gói 6 Tháng' then '6' 
      when product_name = '1.Gói 1 Năm' then '12' 
      when product_name = '5.Free trial' then 'FT'
      end product_type
, sum(qty) total_active
FROM `tiki-dwh.report_view.tikinow_weekly_subscriber`
where product_name <> '6.Total(1st day of week)'
and type_transformation = 'A.Active'	
group by iosyearweek, product_name

),

week_sub as
(select 
customer_id,
max(product_type) product_type,
 concat(cast(EXTRACT(ISOYEAR FROM date) as STRING),'-',
    cast(EXTRACT(ISOWEEK FROM date) as STRING)) Y_W,start_date,end_date,date,fisrt_month
    from raw_2h
    group by customer_id,Y_W,start_date,end_date,date,fisrt_month
),
      
     
sales_order as
(select distinct
    IFNULL(original_increment_id,increment_id) increment_id,
    t1.customer_id,
    date(created_at,"Asia/Ho_Chi_Minh") order_date,
    t2.is_active
from `tiki-dwh.ecom.sales_order_20*` t1
join raw_2h t2 ON t1.customer_id = t2.customer_id and date(created_at,"Asia/Ho_Chi_Minh") = t2.date
where 
_table_suffix >= format_date('%y%m%d',date_TRUNC(date_add(current_date("Asia/Ho_Chi_Minh"), interval -14 WEEK), ISOWEEK))
-- and original_code is null and original_increment_id is null
and status in ('complete','giao_hang_thanh_cong')
),
week_pos as
(select distinct
 concat(cast(EXTRACT(ISOYEAR FROM date) as STRING),'-',
    cast(EXTRACT(ISOWEEK FROM date) as STRING)) Y_W,start_date,end_date
    from raw_2h
),
     
tiki_data as
(
select 
    Y_W iosyearweek,
    count(distinct IFNULL(original_increment_id,increment_id)) total_order_tiki,
    count(distinct customer_id) total_customer_tiki
from 
week_pos tx LEFT JOIN
`tiki-dwh.ecom.sales_order_20*` t1 
ON tx.end_date >= date(created_at,"Asia/Ho_Chi_Minh") and tx.start_date <= date(created_at,"Asia/Ho_Chi_Minh")
WHERE 
_table_suffix >= format_date('%y%m%d',date_TRUNC(date_add(current_date("Asia/Ho_Chi_Minh"), interval -14 WEEK), ISOWEEK))
and status in ('complete','giao_hang_thanh_cong')
group by iosyearweek),

 
final as(
select 
  Y_W,
  product_name,
  total_order total_order_from_subs,
  total_customer total_purchase_customer,
  total_active total_active_subs,
  
  total_order_first total_order_from_subs_firstmonth,
  total_customer_firstmonth total_purchase_customer_firstmonth ,
  total_cus_sub_firstmonth total_active_subs_month,
  
  total_order_tiki,total_customer_tiki
from
(SELECT 
  Y_W,
  product_type,
  count(distinct 
  if(is_active = 1,increment_id,NULL))
  --increment_id) 
  total_order,
  count(distinct customer_id) total_customer,
  count(distinct cus_sub) total_cus_sub,
  
  count(distinct if(is_active = 1 and fisrt_month = 1,customer_id,null)) total_customer_firstmonth,
  count(distinct if(is_active = 1 and fisrt_month = 1,cus_sub,null)) total_cus_sub_firstmonth,
  count(distinct if(is_active = 1 and fisrt_month = 1,increment_id,null)) total_order_first 
    FROM (
        SELECT DISTINCT 
        Y_W,
        increment_id,
        max(product_type) product_type,
        is_active,
        t2.customer_id,
        t1.customer_id cus_sub,
        any_value(fisrt_month) fisrt_month
          From 
            week_sub t1
              left join sales_order t2 ON  t1.customer_id = t2.customer_id
                AND t1.end_date >= order_date and t1.start_date <= order_date
                group by customer_id,Y_W,increment_id,is_active,t1.customer_id)
                group by Y_W,product_type
                order by 
                Y_W) su
                LEFT JOIN tiki_data tk ON su.Y_W = tk.iosyearweek
                LEFT JOIN total_active_all ta ON su.Y_W = ta.iosyearweek and su.product_type = ta.product_type

)

select * from final
