#standardSQL
SELECT  IF(CAST(REGEXP_EXTRACT(iosyearweek,'-(.*)') AS INT64) < 10 ,CONCAT (REGEXP_EXTRACT(iosyearweek,'(.*)-') , '-', '0',REGEXP_EXTRACT(iosyearweek,'-(.*)')), iosyearweek) as  iosyearweek
,Metrics 
,num
FROM 
(

With 
FT60ACC AS(
select 
  'FT_CC' as Metrics,
   concat(cast(EXTRACT(ISOYEAR FROM date(created_at,'+7')) as STRING),'-',cast(EXTRACT(ISOWEEK FROM date(created_at,'+7')) as STRING)) iosyearweek, 
   count(distinct customer_id) as num
from `ecom.customer_free_trial_registration` 
where 
    date(created_at,'+7') >= 
    date_TRUNC(date_add(current_date("Asia/Ho_Chi_Minh"), interval -7 week), WEEK)
    and date(created_at,'+7') <= current_date("Asia/Ho_Chi_Minh")
and payment_method = 'cybersource' 
group by 1,2
),

FT60ANCC AS(
select 
  'FT_NCC' as Metrics,
   concat(cast(EXTRACT(ISOYEAR FROM date(created_at,'+7')) as STRING),'-',cast(EXTRACT(ISOWEEK FROM date(created_at,'+7')) as STRING)) iosyearweek, 
   count(distinct customer_id) as num
from `ecom.customer_free_trial_registration` 
where 
    date(created_at,'+7') >= 
    date_TRUNC(date_add(current_date("Asia/Ho_Chi_Minh"), interval -7 week), WEEK)
    and date(created_at,'+7') <= current_date("Asia/Ho_Chi_Minh")
and payment_method != 'cybersource'
group by 1,2
)

select * from FT60ACC
union all select * from FT60ANCC
)
