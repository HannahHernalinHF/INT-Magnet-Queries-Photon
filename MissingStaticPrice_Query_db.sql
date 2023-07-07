----- Missing Static Pricing -----

WITH isa_recipe_picklist_staticprices AS (
SELECT *,
cast(regexp_replace(regexp_replace(unique_recipe_code, recipe_code, ''),'-','') as int) version
FROM materialized_views.isa_recipe_picklist_staticprices
)

,recipepool_it AS (
SELECT
market,
hellofresh_week,
recipe_status,
unique_recipe_code,
title,
is_default,
sku_status,
IFNULL(CAST(price AS STRING), 'na') AS price,
CONCAT_WS(' , ', COLLECT_LIST(sku_code)) AS sku_code,
CONCAT_WS(' , ', COLLECT_LIST(display_name)) AS sku_name
FROM (
SELECT
sp.*,
Dense_rank() OVER (partition BY sp.recipe_code, sp.market ORDER BY sp.unique_recipe_code DESC) AS o
FROM isa_recipe_picklist_staticprices sp
WHERE lower(sp.recipe_status) in ('ready for menu planning')
AND sp.market='it'
) temp
WHERE temp.o = 1
GROUP BY 1,2,3,4,5,6,7,8
),
recipepool_dkse_no
AS (
SELECT
market,
hellofresh_week,
recipe_status,
unique_recipe_code,
title,
is_default,
sku_status,
IFNULL(CAST(price AS STRING), 'na') AS price,
CONCAT_WS(' , ', COLLECT_LIST(sku_code)) AS sku_code,
CONCAT_WS(' , ', COLLECT_LIST(display_name)) AS sku_name
FROM (
SELECT
sp.*,
Dense_rank() OVER (partition BY sp.recipe_code, sp.market ORDER BY version DESC) AS o
FROM isa_recipe_picklist_staticprices sp
WHERE sp.market = 'dkse'
) temp
WHERE temp.o = 1 AND
lower(temp.recipe_status) not in ('inactive', 'rejected')
GROUP BY 1,2,3,4,5,6,7,8
),
recipepool_gb
AS (
SELECT
market,
hellofresh_week,
recipe_status,
unique_recipe_code,
title,
is_default,
sku_status,
IFNULL(CAST(price AS STRING), 'na') AS price,
CONCAT_WS(' , ', COLLECT_LIST(sku_code)) AS sku_code,
CONCAT_WS(' , ', COLLECT_LIST(display_name)) AS sku_name
FROM (
SELECT
sp.*,
dense_rank() over (partition by sp.recipe_code, sp.market, case when substr(unique_recipe_code, length(unique_recipe_code)-1, length(unique_recipe_code)) in ('FR','CH','DK') then substr(unique_recipe_code, length(unique_recipe_code)-1, length(unique_recipe_code)) else 'X' end order by version desc) as o
FROM isa_recipe_picklist_staticprices sp
WHERE lower(sp.recipe_status) in ('ready for menu planning','final cook')
AND lower(sp.title) not like '%not use%' and lower(sp.title) not like '%wrong%' and lower(sp.title) not like '%test%' and lower(sp.title) not like '%brexit%'
AND sp.unique_recipe_code not like '%MOD%' and sp.unique_recipe_code not like '%ASD%' and sp.unique_recipe_code not like 'GC%'
AND  sp.market='gb'
) temp
WHERE temp.o = 1
GROUP BY 1,2,3,4,5,6,7,8
),
recipepool_fr
AS (
SELECT
market,
hellofresh_week,
recipe_status,
unique_recipe_code,
title,
is_default,
sku_status,
IFNULL(CAST(price AS STRING), 'na') AS price,
CONCAT_WS(' , ', COLLECT_LIST(sku_code)) AS sku_code,
CONCAT_WS(' , ', COLLECT_LIST(display_name)) AS sku_name
FROM (
SELECT
sp.*,
dense_rank() over (partition by sp.recipe_code, sp.market, case when substr(unique_recipe_code, length(unique_recipe_code)-1, length(unique_recipe_code)) in ('FR','CH','DK') then substr(unique_recipe_code, length(unique_recipe_code)-1, length(unique_recipe_code)) else 'X' end order by version desc) as o
FROM    isa_recipe_picklist_staticprices sp
WHERE Lower(sp.recipe_status) IN ('ready for menu planning', 'planned')
AND CASE
WHEN Lower(sp.recipe_status) IN ('ready for menu planning')
THEN sp.unique_recipe_code NOT LIKE '%NL%'
ELSE true
END
AND sp.market='fr'
) temp
WHERE temp.o = 1
GROUP BY 1,2,3,4,5,6,7,8
),
gamp_dach
AS (
SELECT DISTINCT uniquerecipecode FROM (
SELECT * ,dense_rank() over (partition by r.mainrecipecode
, r.country
, case when substr(r.uniquerecipecode
, length(r.uniquerecipecode)-1
, length(r.uniquerecipecode)) in ('FR','CH','DK') then substr(r.uniquerecipecode
, length(r.uniquerecipecode)-1
, length(r.uniquerecipecode)) else 'X' end order by cast(r.version as int) desc) as o
FROM materialized_views.int_scm_analytics_remps_recipe r
WHERE lower(r.status)  in ('ready for menu planning','pool','rework')
and case when
lower(r.status) in ('ready for menu planning','rework')
then versionused is not NULL
else TRUE end
and lower(r.title) not like '%not use%' and lower(r.title) not like '%wrong%'
and length (primaryprotein)>0
and primaryprotein <>'N/A'
AND r.country='DACH'
and substr(r.uniquerecipecode, length(r.uniquerecipecode)-1, length(r.uniquerecipecode))<>'CH'
and r.uniquerecipecode not like 'TEST%'
and r.uniquerecipecode not like 'HE%'
and r.uniquerecipecode not like 'ADD%'
and r.uniquerecipecode not like 'CO%'
and r.uniquerecipecode not like 'XMAS%'
and r.title not like 'PLACEH%'
and case when
lower(r.status)  in ('ready for menu planning')
then r.absolutelastused >='2019-W01'
else TRUE end
) test where o=1
),
recipepool_dach
AS (
SELECT
market,
hellofresh_week,
recipe_status,
unique_recipe_code,
title,
is_default,
sku_status,
IFNULL(CAST(price AS STRING), 'na') AS price,
CONCAT_WS(' , ', COLLECT_LIST(sku_code)) AS sku_code,
CONCAT_WS(' , ', COLLECT_LIST(display_name)) AS sku_name
--currency
FROM (
SELECT *
FROM    isa_recipe_picklist_staticprices sp
WHERE unique_recipe_code IN (SELECT uniquerecipecode FROM gamp_DACH)
and  sp.market='dach'
) temp
GROUP BY 1,2,3,4,5,6,7,8
),
recipepool_ca
AS (
SELECT
market,
hellofresh_week,
recipe_status,
unique_recipe_code,
title,
is_default,
sku_status,
IFNULL(CAST(price AS STRING), 'na') AS price,
CONCAT_WS(' , ', COLLECT_LIST(sku_code)) AS sku_code,
CONCAT_WS(' , ', COLLECT_LIST(display_name)) AS sku_name
FROM (
SELECT
sp.*,
dense_rank() over (partition by sp.recipe_code, sp.market, case when substr(unique_recipe_code, length(unique_recipe_code)-1, length(unique_recipe_code)) in ('FR','CH','DK') then substr(unique_recipe_code, length(unique_recipe_code)-1, length(unique_recipe_code)) else 'X' end order by version desc) as o
FROM    isa_recipe_picklist_staticprices sp
WHERE lower(sp.recipe_status) IN ('ready for menu planning', 'active', 'in development')
AND lower(sp.title) NOT LIKE '%not use%'
AND lower(sp.title) NOT LIKE '%wrong%'
AND lower(sp.title) NOT LIKE '%test%'
AND sp.unique_recipe_code NOT LIKE 'C%'
AND sp.unique_recipe_code NOT LIKE '%-FR'
AND sp.unique_recipe_code NOT LIKE 'ADD%'
AND sp.unique_recipe_code NOT LIKE 'RMOD%'
AND sp.unique_recipe_code NOT LIKE 'RCON%'
AND sp.unique_recipe_code NOT LIKE 'RAO%'
AND sp.unique_recipe_code NOT LIKE 'RRS%'
AND sp.market='ca'
) temp
WHERE temp.o = 1
GROUP BY 1,2,3,4,5,6,7,8
),
recipepool_ck
AS (
SELECT
market,
hellofresh_week,
recipe_status,
unique_recipe_code,
title,
is_default,
sku_status,
IFNULL(CAST(price AS STRING), 'na') AS price,
CONCAT_WS(' , ', COLLECT_LIST(sku_code)) AS sku_code,
CONCAT_WS(' , ', COLLECT_LIST(display_name)) AS sku_name
FROM (
SELECT
sp.*,
dense_rank() over (partition by sp.recipe_code, sp.market, case when substr(unique_recipe_code, length(unique_recipe_code)-1, length(unique_recipe_code)) in ('FR','CH','DK') then substr(unique_recipe_code, length(unique_recipe_code)-1, length(unique_recipe_code)) else 'X' end order by version desc) as o
FROM    isa_recipe_picklist_staticprices sp
WHERE lower(sp.recipe_status) IN ('ready for menu planning', 'in development')
AND lower(sp.title) NOT LIKE '%not use%'
AND lower(sp.title) NOT LIKE '%wrong%'
AND lower(sp.title) NOT LIKE '%covid%'
AND lower(sp.title) NOT LIKE '%tofu%'
AND sp.market='ca'
AND substr(unique_recipe_code, length(unique_recipe_code)-1, length(unique_recipe_code))<>'FR'
AND sp.unique_recipe_code LIKE 'C%'
AND sp.unique_recipe_code NOT LIKE 'TEST%'
) temp
WHERE temp.o = 1
GROUP BY 1,2,3,4,5,6,7,8
),
recipepool_bnl
AS (
SELECT
market,
hellofresh_week,
recipe_status,
unique_recipe_code,
title,
is_default,
sku_status,
IFNULL(CAST(price AS STRING), 'na') AS price,
CONCAT_WS(' , ', COLLECT_LIST(sku_code)) AS sku_code,
CONCAT_WS(' , ', COLLECT_LIST(display_name)) AS sku_name
FROM (
SELECT
sp.*,
dense_rank() over (PARTITION BY sp.recipe_code, sp.market, CASE WHEN substr(unique_recipe_code, length(unique_recipe_code)-1, length(unique_recipe_code)) IN ('FR','CH','DK') THEN substr(unique_recipe_code, length(unique_recipe_code)-1, length(unique_recipe_code)) ELSE 'X' END ORDER BY version DESC) AS o
FROM    isa_recipe_picklist_staticprices sp
WHERE lower(sp.recipe_status) IN ('ready for menu planning', 'under improvement')
AND sp.is_default=true
AND sp.market='beneluxfr'
AND lower(sp.title) NOT LIKE '%not use%'
AND lower(sp.title) NOT LIKE '%wrong%'
AND lower(sp.title) NOT LIKE '%niet%'
AND lower(sp.title) NOT LIKE 'PLACEH%'
AND substr(unique_recipe_code, length(unique_recipe_code)-1, length(unique_recipe_code))<>'FR'
AND sp.unique_recipe_code NOT LIKE 'GC%'
) temp
WHERE temp.o = 1
GROUP BY 1,2,3,4,5,6,7,8
)
--@materialize


SELECT * FROM (
SELECT *
FROM recipepool_it
UNION ALL
SELECT *
FROM recipepool_dkse_no
UNION ALL
SELECT *
FROM recipepool_gb
UNION ALL
SELECT *
FROM recipepool_fr
UNION ALL
SELECT *
FROM recipepool_dach
UNION ALL
SELECT *
FROM recipepool_ca
UNION ALL
SELECT *
FROM recipepool_ck
UNION ALL
SELECT *
FROM recipepool_bnl
) all_data where
hellofresh_week > '2023-W26' AND hellofresh_week < '2023-W39' AND price = 'na' AND sku_status = 'Active' --- Update the hellofresh week depending on the Quarter
