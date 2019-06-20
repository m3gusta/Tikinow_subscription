#standardSQL

WITH raw_data AS (
  SELECT 
  _TABLE_SUFFIX _date,
  concat(cast(EXTRACT(ISOYEAR FROM PARSE_DATE("%y%m%d", CAST(_TABLE_SUFFIX AS STRING))) as STRING),'-',cast(EXTRACT(ISOWEEK FROM PARSE_DATE("%y%m%d", CAST(_TABLE_SUFFIX AS STRING))) as STRING)) isoyearweek,
  hits.page.pagePath,
  CONCAT(fullVisitorId, '-', CAST(visitId AS STRING)) AS unique_visitor_id,
  hits.transaction.transactionId,
  hits.type
  FROM `tiki-gap.129159136.ga_sessions_20*`, UNNEST(hits) hits
  WHERE 
        _TABLE_SUFFIX BETWEEN FORMAT_DATE("%y%m%d", date_TRUNC(date_add(current_date("Asia/Ho_Chi_Minh"), interval -7 week), WEEK)) 
        AND FORMAT_DATE("%y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) 
),
tikinow AS (
  SELECT 
    isoyearweek,
    CASE
        WHEN pagePath LIKE "%header_logo%" THEN "# TikiNOW LP FROM hits - Logo"
        WHEN pagePath LIKE "/tikinow?src=pdp%" THEN "# TikiNOW LP FROM product detal page"
        WHEN pagePath LIKE "/tikinow?src=checkout-payment%" THEN "# TikiNOW LP FROM checkout payment"
        WHEN pagePath LIKE "/tikinow?src=checkout-success-banner%" THEN "# TikiNOW LP FROM check out success banner"
        WHEN pagePath = "/tikinow" THEN "# TikiNOW LP FROM hits - Direct"
        when pagePath = "/tikinow?src=search&searchredirect=tikinow" THEN "# TikiNOW LP FROM search - Direct"	
        ELSE "# TikiNOW LP FROM hits - Others"
    END AS page_path,
    COUNT(DISTINCT unique_visitor_id) AS visit
  FROM raw_data 
  WHERE pagePath LIKE "/tikinow%" 
  GROUP BY 1,2
),
homepage AS (
  SELECT 
    isoyearweek,
    '# LANDINGPAGE hits' AS page_path,
    COUNT(DISTINCT unique_visitor_id) AS visit
  FROM raw_data 
  WHERE pagePath like "%/tikinow%" 
  GROUP BY 1,2
), 
rd AS 
(

SELECT 
  isoyearweek,
  t2.page_path,
  t2.visit AS visit,
  t2.visit/t1.visit AS pct
FROM homepage t1
INNER JOIN tikinow t2 USING(isoyearweek)
UNION ALL
SELECT isoyearweek, page_path, visit, 1 AS pct FROM homepage
) 

SELECT  
IF(CAST(REGEXP_EXTRACT(isoyearweek,'-(.*)') AS INT64) < 10 
    ,CONCAT (REGEXP_EXTRACT(isoyearweek,'(.*)-') , '-', '0'
    ,REGEXP_EXTRACT(isoyearweek,'-(.*)')), isoyearweek) as isoyearweek 
,page_path
,visit
,pct
FROM  rd
