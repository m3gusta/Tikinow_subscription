#standardSQL
SELECT  IF(CAST(REGEXP_EXTRACT(iosyearweek,'-(.*)') AS INT64) < 10 ,CONCAT (REGEXP_EXTRACT(iosyearweek,'(.*)-') , '-', '0',REGEXP_EXTRACT(iosyearweek,'-(.*)')), iosyearweek) as  iosyearweek
,platform 
,num
FROM 
(#standardSQL


select
  
  platform,
--   EXTRACT(ISOYEAR FROM date) AS isoyear,
--   EXTRACT(ISOWEEK FROM date) AS isoweek,
  concat(cast(EXTRACT(ISOYEAR FROM created_at) as STRING),'-',cast(EXTRACT(ISOWEEK FROM created_at) as STRING)) iosyearweek,
  count (distinct customer_id) as num
  from `ecom.customer_free_trial_registration` 
WHERE
    date(created_at,'+7') >= 
    date_TRUNC(date_add(current_date("Asia/Ho_Chi_Minh"), interval -7 week), WEEK)
    and date(created_at,'+7') <= current_date("Asia/Ho_Chi_Minh")
and platform is not null
group by 1,2
)
