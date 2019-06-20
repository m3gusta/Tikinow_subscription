#standardSQL

select
  concat(cast(EXTRACT(ISOYEAR FROM date(created_at,'+7')) as STRING),'-',
  cast(EXTRACT(ISOWEEK FROM date(created_at,'+7')) as STRING)) as iosyearweek,
  sign_up_source,
  count(*) as num
from `tiki-dwh.ecom.customer_free_trial_registration`  
group by 1,2
order by 1 desc

