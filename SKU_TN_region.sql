#standardSQL

with raw as(
select 
  t1.warehouse_id,
  t2.name, t2.code,
  case when 
  seller_id = 1 then '1P'
  else '3P' end as seller,
  t1.product_id

--   count(distinct product_id) as Number_of_product_id
from `ecom.tos_warehouse_stock_item` t1
left join `dwh.dim_warehouse` t2 using(warehouse_id)
left join `dwh.dim_product` t3 using (product_id)
where 1=1 
and product_id in (
          select distinct 
            product_id
          from `ecom.catalog_product_entity_int` 
          where attribute_code = 'support_p2h_delivery'
                     )
and qty_available >= 2
)

select 
  t1.warehouse_id,
  t1.name, t1.code,
  seller, 
  count(5) as Number_of_product_id
from raw t1
group by 1,2,3,4
order by Number_of_product_id desc
