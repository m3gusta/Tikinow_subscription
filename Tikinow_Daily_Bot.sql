#standardSQL

#standardSQL

With 
FTJD as(
select 
  'A0_Free trial join (daily)' as Metrics,
  count (distinct customer_id) as num
  from `ecom.customer_free_trial_registration` 
where date(created_at,'+7') =  date_sub(current_date('+7'), interval 1 day)
),


FTJDIOS as(
select 
  'A1_Free trial join (daily) IOS' as Metrics,
  count (distinct customer_id) as num
  from `ecom.customer_free_trial_registration` 
where date(created_at,'+7') =  date_sub(current_date('+7'), interval 1 day)
and platform = 'mobile-ios'	
),

FTJDAD as(
select 
  'A2_Free trial join (daily) Android' as Metrics,
  count (distinct customer_id) as num
  from `ecom.customer_free_trial_registration` 
where date(created_at,'+7') =  date_sub(current_date('+7'), interval 1 day)
and platform = 'mobile-android'	
),

AMC as(
select 
  'B0_Active members (current)' as Metrics,
  count(distinct customer_id) as num
from `customer.bq_tikinow2h` 
where date = current_date('+7')
and type != 'deactive'
),

-- 60D Free trial
FT60A AS(
select 
  'B10_Free trial 60D' as Metrics,
   count(distinct customer_id) as num
from `ecom.customer_free_trial_registration` 
where date(created_at, '+7') >= date_sub(current_date('+7'), interval 60 day)
),

FT60ACC AS(
select 
  'B11_CC' as Metrics,
   count(distinct customer_id) as num
from `ecom.customer_free_trial_registration` 
where date(created_at, '+7') >= date_sub(current_date('+7'), interval 60 day)
and payment_method = 'cybersource'
),

FT60ANCC AS(
select 
  'B12_NCC' as Metrics,
   count(distinct customer_id) as num
from `ecom.customer_free_trial_registration` 
where date(created_at, '+7') >= date_sub(current_date('+7'), interval 60 day)
and payment_method != 'cybersource'
),

-- sub type

Mo1 AS(
select 
  'B21_Mo1' as Metrics, 
  count(distinct customer_id) as num
from `customer.bq_tikinow2h` 
where type != 'deactive'
and date = date_sub(current_date('+7'), interval 1 day)
and product_name = 'Dịch Vụ TikiNOW (Gói 1 Tháng)'
group by product_name
),

Mo3 AS(
select 
  'B22_Mo3' as Metrics, 
  count(distinct customer_id) as num
from `customer.bq_tikinow2h` 
where type != 'deactive'
and date = date_sub(current_date('+7'), interval 1 day)
and product_name = 'Dịch Vụ TikiNOW (Gói 3 Tháng)'
group by product_name
),

Mo6 AS(
select 
  'B23_Mo6' as Metrics, 
  count(distinct customer_id) as num
from `customer.bq_tikinow2h` 
where type != 'deactive'
and date = date_sub(current_date('+7'), interval 1 day)
and product_name = 'Dịch Vụ TikiNOW (Gói 6 Tháng)'
group by product_name
),

Y1 AS(
select 
  'B24_Y1' as Metrics, 
  count(distinct customer_id) as num
from `customer.bq_tikinow2h` 
where type != 'deactive'
and date = date_sub(current_date('+7'), interval 1 day)
and product_name = 'Dịch Vụ TikiNOW (Gói 1 Năm)'
group by product_name
),


-- TIKINOW Orders delivered daily

temp as(
SELECT
                         o.increment_id,
                         w.code as region,
                         CASE WHEN o.shipping_plan_id = 3 
                         AND o.order_type = 'instock' AND prl2.region_id = prl.region_id THEN 'tikinow'
                         else 'standard' 
                         END as metric,
                         tdo.actual_delivery,
                         CASE 
                         WHEN o.shipping_plan_id = 3 THEN CAST(datetime(o.delivery_commitment_time,'+7') AS datetime)
                         WHEN o.original_increment_id is NULL THEN datetime_add(CAST(FORMAT_DATE('%Y-%m-%d',parse_date("%d/%m/%Y",REGEXP_EXTRACT(o.shipping_description, '(\\d{2}\\/\\d{2}\\/\\d{4})'))) as datetime),interval 1 day)
                         ELSE datetime_add(CAST(FORMAT_DATE('%Y-%m-%d',parse_date("%d/%m/%Y",REGEXP_EXTRACT(oo.shipping_description, '(\\d{2}\\/\\d{2}\\/\\d{4})'))) as datetime),interval 1 day)
                         end delivery_deadline
                         from `ecom.sales_order_2019*` o
                         LEFT JOIN `staging.tiki_delivery_order` tdo ON tdo.order_id = o.entity_id
                         LEFT JOIN `op.dim_warehouse` w ON w.id = o.processing_warehouse_id
                         LEFT JOIN `ecom.sales_order_address` oa ON   oa.entity_id = o.shipping_address_id AND oa.address_type = 'shipping'
                         LEFT JOIN `ecom.sales_order_2019*` oo ON o.original_increment_id = oo.increment_id AND oo.status = 'canceled'
                         LEFT JOIN ecom.province_region_logistics  prl ON prl.region_id = oa.region_id 
                         LEFT JOIN ecom.province_region_logistics  prl2 ON prl2.region_id = w.region_id 
                         WHERE date(tdo.actual_delivery,'Asia/Ho_Chi_Minh') = date_sub(current_date('Asia/Ho_Chi_Minh'),interval 1 day) 
                         AND (o.is_virtual = 0 OR o.is_virtual is NULL)
                         AND o.processing_warehouse_id <> 0 
),

raw as(
SELECT
                         date(actual_delivery, 'Asia/Ho_Chi_Minh') as data_date,
                         region as attribute,
                         count(distinct increment_id) no_orders,
                         if(datetime(actual_delivery,'Asia/Ho_Chi_Minh') <= delivery_deadline,1,0) as ontime
                         FROM
                         (SELECT * from temp
                         where delivery_deadline is not NULL)
                         where metric = 'tikinow'
                         group by data_date, attribute, ontime
                         order by data_date, attribute, ontime
),

final as (
select *,
sum(no_orders) over (partition by data_date, attribute) as total
from raw
),

PCTOTD as (
select 
'D_Percentage on-time (daily)' as Metrics,
round((sum(no_orders) / sum(total))*100,2) as pct
from final
where ontime = 1
group by ontime
),

TNDD as (
select 
'E0_TN deliveries (daily)' as Metrics,
sum(total) as orders
from final
where ontime = 1
-- group by ontime
),

TNDDSGN as (
select 
'E1_SGN' as Metrics,
sum(total) as orders
from final
where 1=1 
and ontime = 1
and (attribute = 'SGN' or attribute = 'VLN')
group by ontime
),

TNDDHN as (
select 
'E2_HN' as Metrics,
sum(total) as orders
from final
where 1=1 
and ontime = 1
and (attribute = 'HN' or attribute ='BFHN')
group by ontime
),

TNDDDN as (
select 
'E3_DN' as Metrics,
sum(total) as orders
from final
where 1=1 
and ontime = 1
and attribute = 'DN'
group by ontime
),

TNDDCT as (
select 
'E4_CT' as Metrics,
sum(total) as orders
from final
where 1=1 
and ontime = 1
and attribute = 'CT'
group by ontime
),

TNDDHP as (
select 
'E5_HP' as Metrics,
sum(total) as orders
from final
where 1=1 
and ontime = 1
and attribute = 'HP'
group by ontime
),

TNDDNT as (
select 
'E6_NT' as Metrics,
sum(total) as orders
from final
where 1=1 
and ontime = 1
and attribute = 'NT'
group by ontime
),

temp2 as(
SELECT
                         o.increment_id,
                         w.code as region,
                         CASE WHEN o.shipping_plan_id = 3 THEN 'tikinow'
--                          AND o.order_type = 'instock' AND prl2.region_id = prl.region_id THEN 'tikinow'
                         else 'standard' 
                         END as metric,
                         tdo.actual_delivery,
                         CASE 
                         WHEN o.shipping_plan_id = 3 THEN CAST(datetime(o.delivery_commitment_time,'+7') AS datetime)
                         WHEN o.original_increment_id is NULL THEN datetime_add(CAST(FORMAT_DATE('%Y-%m-%d',parse_date("%d/%m/%Y",REGEXP_EXTRACT(o.shipping_description, '(\\d{2}\\/\\d{2}\\/\\d{4})'))) as datetime),interval 1 day)
                         ELSE datetime_add(CAST(FORMAT_DATE('%Y-%m-%d',parse_date("%d/%m/%Y",REGEXP_EXTRACT(oo.shipping_description, '(\\d{2}\\/\\d{2}\\/\\d{4})'))) as datetime),interval 1 day)
                         end delivery_deadline
                         from `ecom.sales_order_2019*` o
                         LEFT JOIN `staging.tiki_delivery_order` tdo ON tdo.order_id = o.entity_id
                         LEFT JOIN `op.dim_warehouse` w ON w.id = o.processing_warehouse_id
                         LEFT JOIN `ecom.sales_order_address` oa ON   oa.entity_id = o.shipping_address_id AND oa.address_type = 'shipping'
                         LEFT JOIN `ecom.sales_order_2019*` oo ON o.original_increment_id = oo.increment_id AND oo.status = 'canceled'
--                          LEFT JOIN ecom.province_region_logistics  prl ON prl.region_id = oa.region_id 
--                          LEFT JOIN ecom.province_region_logistics  prl2 ON prl2.region_id = w.region_id 
                         WHERE date(tdo.actual_delivery,'Asia/Ho_Chi_Minh') = date_sub(current_date('Asia/Ho_Chi_Minh'),interval 1 day) 
                         AND (o.is_virtual = 0 OR o.is_virtual is NULL)
                         AND o.processing_warehouse_id <> 0 
),

raw2 as(
SELECT
                         date(actual_delivery, 'Asia/Ho_Chi_Minh') as data_date,
                         region as attribute,
                         count(distinct increment_id) no_orders,
                         if(datetime(actual_delivery,'Asia/Ho_Chi_Minh') <= delivery_deadline,1,0) as ontime
                         FROM
                         (SELECT * from temp2
                         where delivery_deadline is not NULL)
                         where metric = 'tikinow'
                         group by data_date, attribute, ontime
                         order by data_date, attribute, ontime
),

final2 as (
select *,
sum(no_orders) over (partition by data_date, attribute) as total
from raw2
),

TNDD2 as (
select 
'E0.1_TN ordered (daily)' as Metrics,
sum(total) as orders
from final2
where ontime = 1
-- group by ontime
),


#---------------------------------------------------------------PART2--------------------------------------------------------------------------# 

TNEXP as(
select
  'F1.0_TN expiration (daily)' as Metrics,
  count(distinct customer_id) as Number_of_expired_subscriber
from `ecom.customer_subscription` 
where id in (select tikinow_id from `ecom.customer_free_trial_registration` )
and date(end_date,'+7') = date_sub(current_date('+7'), interval 1 day)
-- and date(end_date,'+7') <= current_date('+7')
group by 1
order by 1 desc
),

TNEXPCC as(
select
  'F1.1_TN expiration_CC (daily)' as Metrics,
  count(distinct customer_id) as Number_of_expired_subscriber
from `ecom.customer_subscription` 
where id in (select tikinow_id from `ecom.customer_free_trial_registration` where payment_method = 'cybersource')
and date(end_date,'+7') = date_sub(current_date('+7'), interval 1 day)
-- and date(end_date,'+7') <= current_date('+7')
group by 1
order by 1 desc
),

TNEXPNCC as(
select
  'F1.2_TN expiration_NCC (daily)' as Metrics,
  count(distinct customer_id) as Number_of_expired_subscriber
from `ecom.customer_subscription` 
where id in (select tikinow_id from `ecom.customer_free_trial_registration` where payment_method != 'cybersource')
and date(end_date,'+7') = date_sub(current_date('+7'), interval 1 day)
-- and date(end_date,'+7') <= current_date('+7')
group by 1
order by 1 desc
),

-- # Number of retention subscribers

all_FT_subs as(
select
  t1.customer_id,
  t1.tikinow_id,
  t1.created_at,
  t1.id as free_trial_id,
  t2.id as id2,
  t2.customer_id as cus2,
  t2.start_date,
  t2.end_date,
  t2.status,
  t1.payment_method
from `ecom.customer_free_trial_registration` t1
left join `ecom.customer_subscription` t2 on t1.customer_id = t2.customer_id
where tikinow_id <= t2.id
order by customer_id, created_at 
),

Renewal_id as(
select
  id2
from all_FT_subs 
where id2 not in (select tikinow_id from `ecom.customer_free_trial_registration`)
),

TNRENEWALA as(
select
  'F2.0_TN renewal (daily)' as Metrics,
  count(distinct customer_id) as number_of_subscribers
--   *, timestamp_diff(end_date,start_date, day) as num
from `ecom.customer_subscription` where id in (select id2 from Renewal_id) 
and status = 1
-- and timestamp_diff(end_date,start_date, day) = 60
and date(created_at,'+7') = date_sub(current_date('+7'), interval 1 day)
group by 1
order by 1
),

# CC
Renewal_id_CC as(
select
  id2
from all_FT_subs 
where id2 not in (select tikinow_id from `ecom.customer_free_trial_registration`)
and payment_method = 'cybersource'
),

TNRENEWALCC as(
select
  'F2.1_TN renewal_CC (daily)' as Metrics,
  count(distinct customer_id) as number_of_subscribers
--   *, timestamp_diff(end_date,start_date, day) as num
from `ecom.customer_subscription` where id in (select id2 from Renewal_id_CC) 
and status = 1
-- and timestamp_diff(end_date,start_date, day) = 60
and date(created_at,'+7') = date_sub(current_date('+7'), interval 1 day)
-- group by 1
-- order by 1
),

#NCC
Renewal_id_NCC as(
select
  id2
from all_FT_subs 
where id2 not in (select tikinow_id from `ecom.customer_free_trial_registration`)
and payment_method != 'cybersource'
),

TNRENEWALNCC as(
select
  'F2.2_TN renewal_NCC (daily)' as Metrics,
  count(distinct customer_id) as number_of_subscribers
--   *, timestamp_diff(end_date,start_date, day) as num
from `ecom.customer_subscription` where id in (select id2 from Renewal_id_NCC) 
and status = 1
-- and timestamp_diff(end_date,start_date, day) = 60
and date(created_at,'+7') = date_sub(current_date('+7'), interval 1 day)
-- group by 1
-- order by 1
),

TNEXPTOTAL as(
select
  'F3.0_TN expiration (total)' as Metrics,
  count(distinct customer_id) as Number_of_expired_subscriber
from `ecom.customer_subscription` 
where id in (select tikinow_id from `ecom.customer_free_trial_registration` )
-- and date(end_date,'+7') = date_sub(current_date('+7'), interval 1 day)
and date(end_date,'+7') < current_date('+7')
-- group by 1
-- order by 1 desc
),

TNEXPTOTALCC as(
select
  'F3.1_TN expiration_CC (total)' as Metrics,
  count(distinct customer_id) as Number_of_expired_subscriber
from `ecom.customer_subscription` 
where id in (select tikinow_id from `ecom.customer_free_trial_registration` where payment_method = 'cybersource' )
-- and date(end_date,'+7') = date_sub(current_date('+7'), interval 1 day)
and date(end_date,'+7') < current_date('+7')
-- group by 1
-- order by 1 desc
),

TNEXPTOTALNCC as(
select
  'F3.2_TN expiration_NCC (total)' as Metrics,
  count(distinct customer_id) as Number_of_expired_subscriber
from `ecom.customer_subscription` 
where id in (select tikinow_id from `ecom.customer_free_trial_registration` where payment_method != 'cybersource')
-- and date(end_date,'+7') = date_sub(current_date('+7'), interval 1 day)
and date(end_date,'+7') < current_date('+7')
-- group by 1
-- order by 1 desc
),

TNRENEWALTOTALA as(
select
  'F4.0_TN renewal (total)' as Metrics,
  count(distinct customer_id) as number_of_subscribers
--   *, timestamp_diff(end_date,start_date, day) as num
from `ecom.customer_subscription` where id in (select id2 from Renewal_id) 
and status = 1
-- and timestamp_diff(end_date,start_date, day) = 60
and date(created_at,'+7') < current_date('+7')
-- group by 1
-- order by 1
),

TNRENEWALTOTALCC as(
select
  'F4.1_TN renewal_CC' as Metrics,
  count(distinct customer_id) as number_of_subscribers
--   *, timestamp_diff(end_date,start_date, day) as num
from `ecom.customer_subscription` where id in (select id2 from Renewal_id_CC) 
and status = 1
-- and timestamp_diff(end_date,start_date, day) = 60
and date(created_at,'+7') < current_date('+7')
-- group by 1
-- order by 1
),

TNRENEWALTOTALNCC as(
select
  'F4.2_TN renewal_NCC' as Metrics,
  count(distinct customer_id) as number_of_subscribers
--   *, timestamp_diff(end_date,start_date, day) as num
from `ecom.customer_subscription` where id in (select id2 from Renewal_id_NCC) 
and status = 1
-- and timestamp_diff(end_date,start_date, day) = 60
and date(created_at,'+7') < current_date('+7')
-- group by 1
-- order by 1
),

TNREMINDD as(
select 
  'G1.0_TN Remind_Daily' as Metrics,
  count(distinct customer_id) as number
from `ecom.customer_tikinow_renewal_reminder` 
where date(popup_last_reminded_at,'+7') = date_sub(current_date('+7'), interval 1 day)
),

TNREMINDALL as(
select
  'G1.1_TN Remind_Total' as Metrics,
  count(distinct customer_id) as number
from `ecom.customer_tikinow_renewal_reminder` 
where date(popup_last_reminded_at,'+7') <= date_sub(current_date('+7'), interval 1 day)
)



select * from FTJD
union all select * from FTJDIOS
union all select * from FTJDAD
union all select * from AMC
union all select * from FT60A
union all select * from FT60ACC
union all select * from FT60ANCC
union all select * from Mo1
union all select * from Mo3
union all select * from Mo6
union all select * from Y1
union all select * from PCTOTD
union all select * from TNDD
union all select * from TNDD2
union all select * from TNDDSGN
union all select * from TNDDHN
union all select * from TNDDDN
union all select * from TNDDCT
union all select * from TNDDHP
union all select * from TNDDNT
union all select * from TNEXP
union all select * from TNEXPCC
union all select * from TNEXPNCC
union all select * from TNRENEWALA
union all select * from TNRENEWALCC
union all select * from TNRENEWALNCC
union all select * from TNEXPTOTAL
union all select * from TNEXPTOTALCC
union all select * from TNEXPTOTALNCC
union all select * from TNRENEWALTOTALA
union all select * from TNRENEWALTOTALCC
union all select * from TNRENEWALTOTALNCC
union all select * from TNREMINDD
union all select * from TNREMINDALL
order by Metrics ASC
