#standardSQL

with raw as(
select
  t1.customer_id,
  t1.tikinow_id,
  t1.created_at,
  t1.id as free_trial_id,
  t2.id as id2,
  t2.customer_id as cus2,
  t2.start_date,
  t2.end_date,
  t2.status
from `ecom.customer_free_trial_registration` t1
left join `ecom.customer_subscription` t2 on t1.customer_id = t2.customer_id
where tikinow_id <= t2.id
and t1.payment_method != 'cybersource'
order by customer_id, created_at 
),

subscription_dates AS (
  SELECT customer_id , active_date
  FROM raw, 
    UNNEST(
      GENERATE_DATE_ARRAY(
        DATE(start_date, "Asia/Ho_Chi_Minh"), 
        DATE(end_date, "Asia/Ho_Chi_Minh")
      )
    ) active_date
  WHERE status = 1 AND 
    customer_id IN (
      SELECT customer_id FROM (
        SELECT customer_id, start_date, end_date,
            ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY start_date ASC) AS row_num
        FROM raw
        ORDER BY start_date ASC 
      ) 
      WHERE DATE_DIFF(DATE(end_date, "Asia/Ho_Chi_Minh"), DATE(start_date, "Asia/Ho_Chi_Minh"), DAY) = 60
      AND row_num = 1
    )
    AND active_date <= CURRENT_DATE()
),


data as(
  SELECT DISTINCT
  customer_id,
  DATE_TRUNC(active_date, WEEK(MONDAY)) active_date
  FROM subscription_dates  sd),
  
release as(
SELECT customer_id,
  min(active_date) OVER (PARTITION BY customer_id) first_week,
  active_date in_week,
  count(distinct  customer_id) OVER (PARTITION BY active_date) customer_in_week
FROM data
)

select distinct
first_week,
in_week,
customer_in_week,
DATE_DIFF(in_week,first_week,ISOWEEK) week_number,
count(1) OVER (PARTITION BY first_week,in_week) retention,
count(1) OVER (PARTITION BY first_week,in_week)/customer_in_week * 100 grow_up_percent,
count(distinct customer_id) OVER (PARTITION BY first_week) customer_in_first_week,
count(1) OVER (PARTITION BY first_week,in_week)/count(distinct customer_id) OVER (PARTITION BY first_week) * 100 retention_percent
from release
order by first_week,in_week
