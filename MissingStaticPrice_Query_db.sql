----- Missing Static Pricing -----
/*
IMPORTANT NOTE: before running the query, update the HelloFresh weeks found in the last lines
depending on the preferred weeks EX: If the data to extract is for Q2 of 2023 then update the weeks to '2023-W13' AND '2023-W26'
*/

WITH isa_recipe_picklist_staticprices AS (
SELECT *,
CAST(REGEXP_REPLACE(REGEXP_REPLACE(unique_recipe_code, recipe_code, ''), '-', '') AS INT) version
FROM materialized_views.isa_recipe_picklist_staticprices
)
, missing_static_price_INT AS (
SELECT
UPPER(market) AS market,
hellofresh_week,
recipe_status,
unique_recipe_code,
title,
is_default,
sku_status,
IFNULL(CAST(price AS STRING), 'na') AS price,
CONCAT_WS(' , ', COLLECT_LIST(DISTINCT sku_code)) AS skucode_mp,
CONCAT_WS(' , ', COLLECT_LIST(DISTINCT display_name)) AS skuname_mp
FROM (
SELECT
sp.*,
DENSE_RANK() OVER (PARTITION BY sp.recipe_code, sp.market ORDER BY version DESC) AS o
FROM isa_recipe_picklist_staticprices AS sp
WHERE
(sp.market='dkse' AND LOWER(sp.recipe_status) NOT IN ('inactive','rejected'))
OR
(sp.market='fr'
AND LOWER(recipe_status) IN ('ready for menu planning', 'planned')
AND (LOWER(recipe_status) = 'ready for menu planning' OR unique_recipe_code NOT LIKE '%NL%')
)
OR
(sp.market='gb' AND LOWER(sp.recipe_status) IN ('ready for menu planning','final cook')
AND LOWER(sp.title) NOT LIKE '%not use%'
AND LOWER(sp.title) NOT LIKE '%wrong%'
AND LOWER(sp.title) NOT LIKE '%test%'
AND LOWER(sp.title) NOT LIKE '%brexit%'
AND sp.unique_recipe_code NOT LIKE '%MOD%'
AND sp.unique_recipe_code NOT LIKE '%ASD%'
AND sp.unique_recipe_code NOT LIKE 'GC%'
)
OR
(sp.market='it' AND LOWER(sp.recipe_status)='ready for menu planning')
OR
(sp.market='ca'
AND LOWER(sp.recipe_status) IN ('ready for menu planning', 'active', 'in development')
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
)
) temp
WHERE temp.o = 1
AND LOWER(temp.title) NOT LIKE '%placeh%'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)
, missing_static_price_DACH AS (
SELECT
UPPER(market) AS market,
hellofresh_week,
recipe_status,
unique_recipe_code,
title,
is_default,
sku_status,
IFNULL(CAST(price AS STRING), 'na') AS price,
CONCAT_WS(' , ', COLLECT_LIST(DISTINCT sku_code)) AS skucode_mp,
CONCAT_WS(' , ', COLLECT_LIST(DISTINCT display_name)) AS skuname_mp
FROM isa_recipe_picklist_staticprices
WHERE unique_recipe_code IN (
SELECT DISTINCT uniquerecipecode
FROM materialized_views.int_scm_analytics_remps_recipe
WHERE LOWER(status) IN ('ready for menu planning', 'pool', 'rework')
AND (status = 'ready for menu planning' OR versionused IS NOT NULL)
AND LOWER(title) NOT LIKE '%not use%'
AND LOWER(title) NOT LIKE '%wrong%'
AND LENGTH(primaryprotein) > 0
AND primaryprotein <> 'N/A'
AND country = 'DACH'
AND substr(uniquerecipecode, length(uniquerecipecode)-1, length(uniquerecipecode)) <> 'CH'
AND uniquerecipecode NOT LIKE 'TEST%'
AND uniquerecipecode NOT LIKE 'HE%'
AND uniquerecipecode NOT LIKE 'ADD%'
AND uniquerecipecode NOT LIKE 'CO%'
AND uniquerecipecode NOT LIKE 'XMAS%'
AND title NOT LIKE 'PLACEH%'
AND (status = 'ready for menu planning' OR absolutelastused >= '2019-W01')
)
AND market = 'dach'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)
, missing_static_price AS (
SELECT * FROM missing_static_price_INT
UNION ALL
SELECT * FROM missing_static_price_DACH
)
SELECT *
FROM missing_static_price
WHERE hellofresh_week BETWEEN '2023-W26' AND '2023-W39' --- update with the preferred weeks
AND price = 'na'
AND sku_status = 'Active'

--- created by @Hannah Hernalin as of 7 JUL 2023
