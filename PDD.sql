#standardSQL
with temp as(
SELECT
                         o.increment_id,
                         w.code as region,
                         CASE WHEN o.shipping_plan_id = 3 then 'tikinow'
                         else 'standard' 
                         END as metric,
                         tdo.actual_delivery,
                         CASE 
                         WHEN o.shipping_plan_id = 3 THEN CAST(datetime(o.delivery_commitment_time,'+7') AS datetime)
                         WHEN o.original_increment_id is NULL THEN datetime_add(CAST(FORMAT_DATE('%Y-%m-%d',parse_date("%d/%m/%Y",REGEXP_EXTRACT(o.shipping_description, '(\\d{2}\\/\\d{2}\\/\\d{4})'))) as datetime),interval 1 day)
                         ELSE datetime_add(CAST(FORMAT_DATE('%Y-%m-%d',parse_date("%d/%m/%Y",REGEXP_EXTRACT(oo.shipping_description, '(\\d{2}\\/\\d{2}\\/\\d{4})'))) as datetime),interval 1 day)
                         end delivery_deadline
                         from `ecom.sales_order_2019*` o
                         LEFT JOIN `ecom.tiki_delivery_order_2019*` tdo ON tdo.order_id = o.entity_id
                         LEFT JOIN `op.dim_warehouse` w ON w.id = o.processing_warehouse_id
                         LEFT JOIN `ecom.sales_order_2019*` oo ON o.original_increment_id = oo.increment_id AND oo.status = 'canceled'
                         WHERE
--                          o._TABLE_SUFFIX >= '180601'
--                          AND tdo._TABLE_SUFFIX >= '180601'
--                          AND oo._TABLE_SUFFIX >= '180601'
--                          AND 
                         datetime(tdo.actual_delivery,'Asia/Ho_Chi_Minh')  between datetime_sub(current_datetime('Asia/Ho_Chi_Minh'),interval 50 day) 
                         and current_datetime('Asia/Ho_Chi_Minh')
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
if(extract(isoweek from data_date) < 10, 
concat(cast(extract(year from data_date) as string), "-0", cast(extract(isoweek from data_date) as string)),
concat(cast(extract(year from data_date) as string), "-", cast(extract(isoweek from data_date) as string))) as yearweek,
sum(no_orders) over (partition by data_date, attribute) as totalbyday
from raw
),

final2 as(
select *,
sum(no_orders) over (partition by yearweek, attribute, ontime) as no_ordersbyweek,
sum(no_orders) over (partition by yearweek, attribute) as totalbyweek,
sum(no_orders) over (partition by yearweek, ontime) as all_no_ordersbyweek,
sum(no_orders) over (partition by yearweek ) as all_totalbyweek
from final
)

select *,
no_orders/ totalbyday as pctbyday,
no_ordersbyweek/ totalbyweek as pctbyweek,
all_no_ordersbyweek/ all_totalbyweek as all_pctbyweek
from final2
