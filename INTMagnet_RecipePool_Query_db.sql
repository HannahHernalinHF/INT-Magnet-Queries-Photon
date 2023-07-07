----- INT Magnet Recipe Pool Query -----
---FROM original queries by GAMP AND with modifications/additions made by @Hannah Hernalin----
--- Updated as of 19 JUN 2023 ---


-------------------- COSTS & SKU COSTS CTEs --------------------

WITH sku_cost_CPS AS (
    SELECT market
         , distribution_center
         , code
         , avg(price) AS price
    FROM materialized_views.procurement_services_staticprices AS sp
    LEFT JOIN materialized_views.procurement_services_culinarysku AS sku
        ON sku.id=sp.culinary_sku_id
    WHERE  sku.market IN ('dkse','es','gb','ie','it','beneluxfr')
        AND sp.distribution_center IN ('SK','SP','GR','IE','IT','DH')
        AND sp.hellofresh_week >= '2023-W13'
        AND sp.hellofresh_week <= '2023-W26'
    GROUP BY 1,2,3
    )



    ,last_sku_cost_remps AS (
        SELECT market, code, status, avg(price) AS price
        FROM materialized_views.procurement_services_staticprices AS sp
        LEFT JOIN materialized_views.procurement_services_culinarysku AS sku
            ON sku.id=sp.culinary_sku_id
        WHERE  sku.market IN ('ca') AND sp.hellofresh_week>='2023-W13' AND sp.hellofresh_week<='2023-W26' --AND sp.distribution_center='OA'
        GROUP BY 1,2,3
    )


    , last_cost_remps as(
    select *
    from (
    select *,
            dense_rank() over(partition by remps_instance order by fk_imported_at desc) AS o
    from remps.recipe_recipecost
    where remps_instance IN ('DACH')
    )t where o=1
    )


-------------------- NUTRITION CTEs --------------------

    , nutrition_CPS AS (
        SELECT *
        FROM materialized_views.culinary_services_recipe_segment_nutrition
        WHERE (market IN ('dkse','it','ie','es','fr') AND segment IN ('SE','IT','IE','ES','FR'))
            OR (market='gb' AND country = 'GB')
     )


    , last_nutrition_remps AS (
        SELECT *
        FROM (
            SELECT *,
            DENSE_RANK() OVER(PARTITION BY remps_instance ORDER BY fk_imported_at DESC) AS o
            FROM remps.recipe_nutritionalinfopp
            WHERE remps_instance IN ('CA','DACH')
        ) AS t
        WHERE o = 1)




-------------------- RECIPE CONSOLIDATED CTE --------------------


    , recipe_consolidated_CPS AS
        (SELECT DISTINCT id,market,unique_recipe_code,recipe_code,version,status,is_default,title,subtitle,recipe_type,brand,
                        cloned_FROM,cloned_version,target_preferences,target_products,tags,label,difficulty,cooking_methods,
                        cuisine,dish_type,spiciness,primary_protein,primary_starch,primary_vegetable,primary_cheese,primary_dairy,
                        primary_dry_spice,primary_fresh_herb,primary_fruit,sauce_paste,secondary_protein,secondary_starch,
                        secondary_vegetable,secondary_cheese,secondary_dairy,secondary_dry_spice,secondary_fresh_herb,secondary_fruit,
                        tertiary_vegetable,main_protein,protein_cut,main_starch,main_vegetable,proteins,starches,vegetables,
                        used_week,total_time,hands_off_time,hands_off_time_max,hands_on_time,hands_on_time_max,active_cooking_time,
                        active_cooking_time_max,passive_cooking_time,passive_cooking_time_max,prep_time,prep_time_max,fun_fact_title,
                        fun_fact_description,created_by,main_image,image_url,internal_image,prep_info,chefs_notes,event_type,
                        created_at,published_at,updated_at,fk_imported_at,inserted_at
        FROM materialized_views.isa_services_recipe_consolidated)




-------------------- STEPS CTE --------------------




    , steps_CPS AS (
        SELECT r.id,
            steps.recipe_id,
            r.market,
            r.unique_recipe_code,
            concat_ws(" | ", collect_list(steps.description)) AS step_description
        FROM materialized_views.culinary_services_recipe_steps_translations AS steps
        JOIN recipe_consolidated_CPS AS r
            ON r.id = steps.recipe_id AND r.market = steps.market
        WHERE r.market IN ('dkse','es','fr','gb','ie','it')
        GROUP BY 1,2,3,4
        )


-------------------- ALLERGENS CTE --------------------


    , allergens AS (SELECT recipe_id
                , market
                , unique_recipe_code
                , previous_allergens
                , current_allergens
                , CASE WHEN change_in_culinary_sku='added' AND allergen_added != ''
                        THEN CONCAT('added:',allergen_added)
                    WHEN change_in_culinary_sku='removed' AND allergen_removed != ''
                        THEN CONCAT('removed:',allergen_removed)
                    ELSE "no change"
                    END AS allergen_change
                , CAST (updated_at AS timestamp) AS allergen_updated_at
                --, RANK() OVER (PARTITION BY unique_recipe_code ORDER BY updated_at DESC) AS max_update
    FROM uploads.isa__allergen_change
    WHERE updated_at = (SELECT max(updated_at) FROM uploads.isa__allergen_change)
        --AND allergen_added like '%hazelnoten%'
    ORDER BY unique_recipe_code,updated_at)


-------------------- REMPS CTEs --------------------




, last_recipe_remps AS (
SELECT *
FROM (
SELECT *,
        DENSE_RANK() OVER(PARTITION BY remps_instance ORDER BY fk_imported_at DESC) AS o
FROM remps.recipe_recipes
WHERE remps_instance IN ('CA','DACH')
)t WHERE o=1
)




, last_product_remps AS (
SELECT *
FROM (
SELECT *,
        DENSE_RANK() OVER(PARTITION BY remps_instance ORDER BY fk_imported_at DESC) AS o
FROM remps.recipe_producttypes
WHERE remps_instance IN ('CA','DACH')
)t WHERE o=1
)


, last_preference_remps AS (
SELECT *
FROM (
SELECT *,
        DENSE_RANK() OVER(PARTITION BY remps_instance ORDER BY fk_imported_at DESC) AS o
FROM remps.recipetags_recipepreferences
WHERE remps_instance IN ('CA','DACH')
)t WHERE o=1
)


, last_preference_map_remps AS (
SELECT *
FROM (
SELECT *,
        DENSE_RANK() OVER(PARTITION BY remps_instance ORDER BY fk_imported_at DESC) AS o
FROM remps.map_recipepreferences_recipes
WHERE remps_instance IN ('CA','DACH')
)t WHERE o=1
)


,last_ingredient_group_remps AS (
    SELECT *
    FROM (
        SELECT *,
        DENSE_RANK() OVER(PARTITION BY remps_instance ORDER BY fk_imported_at DESC) AS o
        FROM remps.recipe_ingredientgroup
    WHERE remps_instance IN ('CA','DACH')
    ) AS t
WHERE o = 1)


, last_recipe_sku_remps AS (
SELECT *
    FROM (
        SELECT *,
        DENSE_RANK() OVER(PARTITION BY remps_instance ORDER BY fk_imported_at DESC) AS o
        FROM remps.recipe_recipeskus
    WHERE remps_instance IN ('CA','DACH')
    ) AS t
WHERE o = 1)


,last_sku_remps AS (
SELECT *
FROM (
        SELECT *,
        DENSE_RANK() OVER(PARTITION BY remps_instance ORDER BY fk_imported_at DESC) AS o
        FROM remps.sku_sku
    WHERE remps_instance IN ('CA','DACH')
    ) AS t
WHERE o = 1)




, last_hqtag_remps AS (
SELECT *
FROM (
SELECT *,
        DENSE_RANK() OVER(PARTITION BY remps_instance ORDER BY fk_imported_at DESC) AS o
FROM remps.recipetags_hqtags
WHERE remps_instance IN ('CA','DACH')
)t WHERE o=1
)


, last_hqtag_map_remps AS (
SELECT *
FROM (
SELECT *,
        DENSE_RANK() OVER(PARTITION BY remps_instance ORDER BY fk_imported_at DESC) AS o
FROM remps.map_hqtags_recipes
WHERE remps_instance IN ('CA','DACH')
)t WHERE o=1
)


,hqtag_remps AS (
SELECT rr.remps_instance,rr.unique_recipe_code AS uniquerecipecode, coalesce(concat_ws(',', collect_set(DISTINCT rt.original_name)), '') AS name
FROM last_recipe_remps rr
LEFT JOIN last_hqtag_map_remps m ON rr.id= m.recipe_recipes_id
LEFT JOIN last_hqtag_remps rt ON rt.id=m.recipetags_hqtags_id
GROUP BY 1,2
)


, preference_remps AS (
SELECT rr.remps_instance, rr.unique_recipe_code AS uniquerecipecode, COALESCE(concat_ws(',', collect_set(DISTINCT rp.name)), '') AS name
FROM last_recipe_remps rr
LEFT JOIN last_preference_map_remps m ON rr.id= m.recipe_recipes_id
LEFT JOIN last_preference_remps rp ON rp.id=m.recipetags_recipepreferences_id
GROUP BY 1,2
)


,producttype_remps AS (
SELECT rr.remps_instance,rr.unique_recipe_code AS uniquerecipecode, COALESCE(concat_ws(',', collect_set(DISTINCT rp.name)), '') AS name
FROM last_recipe_remps rr
LEFT JOIN last_product_remps rp ON rp.id=rr.recipe__product_type
GROUP BY 1,2
)


, last_tag_map_remps AS (
SELECT *
FROM (
SELECT *,
        DENSE_RANK() OVER(PARTITION BY remps_instance ORDER BY fk_imported_at DESC) AS o
FROM remps.map_tags_recipes
WHERE remps_instance IN ('CA','DACH')
)t WHERE o=1
)

, last_tag_remps AS (
SELECT *
FROM (
SELECT *,
        DENSE_RANK() OVER(PARTITION BY remps_instance ORDER BY fk_imported_at DESC) AS o
FROM remps.recipetags_tags
WHERE remps_instance IN ('CA','DACH')
)t WHERE o=1
)


, tag_remps AS (
SELECT rr.remps_instance,rr.unique_recipe_code AS uniquerecipecode, COALESCE(concat_ws(',', collect_set(DISTINCT rt.name)), '') AS name
FROM last_recipe_remps AS rr
LEFT JOIN last_tag_map_remps AS m ON rr.id= m.recipe_recipes_id
LEFT JOIN last_tag_remps AS rt ON rt.id=m.recipetags_tags_id
GROUP BY 1,2
)



-------------------- 2P SKU COUNT CTE --------------------


 , skucount_2p_CPS AS (
        SELECT market
                , segment_name
                , unique_recipe_code
                , concat_ws(" | ", collect_list(NAME)) AS skuname
                , count(DISTINCT code) AS skucount
                , concat_ws(" | ", collect_list(status)) AS sku_status
                , size
        FROM (
            SELECT r.market
                , p.segment_name
                , r.unique_recipe_code
                , p.code
                , regexp_replace(p.name, '\t|\n', '') AS NAME
                , skus.status
                , p.size
            FROM recipe_consolidated_CPS AS r
            JOIN materialized_views.culinary_services_recipe_procurement_picklist_culinarysku AS p
                ON r.id = p.recipe_id
            LEFT JOIN materialized_views.procurement_services_culinarysku AS skus
               ON skus.id = p.culinarysku_id
            WHERE r.market IN ('dkse','it','ie','gb','es')
            AND p.segment_name IN ('SE', 'IT','IE','GR','ES')
            AND p.size = 2
            GROUP BY 1, 2, 3, 4, 5, 6, 7) t
        GROUP BY 1,2,3,7
    )


 , skucount_2p_FR AS (
        SELECT unique_recipe_code
                , concat_ws(" | ", collect_list(NAME)) AS skuname
                , count(DISTINCT code) AS skucount
                , concat_ws(" | ", collect_list(status)) AS sku_status
                , size
        FROM (
            SELECT r.unique_recipe_code
                , p.code
                , regexp_replace(p.name, '\t|\n', '') AS NAME
                , skus.status
                , p.size
            FROM recipe_consolidated_CPS AS r
            JOIN materialized_views.culinary_services_recipe_procurement_picklist_culinarysku AS p
                ON r.id = p.recipe_id
            LEFT JOIN materialized_views.procurement_services_culinarysku AS skus
                    ON p.code = skus.code AND skus.market = 'beneluxfr'
            WHERE r.market = 'fr'
            AND p.segment_name = 'FR'
            AND p.size = 2
            GROUP BY 1, 2, 3, 4, 5) t
        GROUP BY 1,5
    )


-------------------- INACTIVE SKUS CTEs --------------------


, inactiveskus_CPS AS (
        SELECT market,
            segment_name,
            unique_recipe_code,
            concat_ws(" | ", collect_list(skucode)) AS inactiveskus,
            concat_ws(" | ", collect_list(skuname)) AS inactiveskuname,
            count(skuname) AS inactiveskus_count
        FROM (
                SELECT r.market
                    , p.segment_name
                    , r.unique_recipe_code
                    , p.code AS skucode
                    , regexp_replace(p.name, '\t|\n', '') AS skuname
                    , skus.status
                    , p.size
                FROM recipe_consolidated_CPS AS r
                JOIN materialized_views.culinary_services_recipe_procurement_picklist_culinarysku AS p
                    ON r.id = p.recipe_id
                LEFT JOIN materialized_views.procurement_services_culinarysku AS skus
                    ON skus.id = p.culinarysku_id
                WHERE r.market IN ('dkse','it','ie','gb','es')
                AND p.segment_name IN ('SE', 'IT','IE','GR','ES')
                AND skus.status LIKE  '%Inactive%' OR skus.status LIKE  '%Archived%'
                AND p.size = 2
                GROUP BY 1, 2, 3, 4, 5, 6, 7
            ) t
        GROUP BY 1,2,3
        )


, inactiveskus_FR AS (
        SELECT market,
            segment_name,
            unique_recipe_code,
            concat_ws(" | ", collect_list(skucode)) AS inactiveskus,
            concat_ws(" | ", collect_list(skuname)) AS inactiveskusnames,
            count(skuname) AS inactiveskus_count
        FROM (
                SELECT r.market
                    , p.segment_name
                    , r.unique_recipe_code
                    , p.code AS skucode
                    , regexp_replace(p.name, '\t|\n', '') AS skuname
                    , skus.status
                    , p.size
                FROM recipe_consolidated_CPS AS r
                JOIN materialized_views.culinary_services_recipe_procurement_picklist_culinarysku AS p
                    ON r.id = p.recipe_id AND r.market = p.market
                JOIN materialized_views.procurement_services_culinarysku AS skus
                    ON p.code = skus.code AND skus.market = 'beneluxfr'
                WHERE r.market = 'fr'
                AND p.segment_name = 'FR'
                AND skus.status LIKE '%Inactive%' OR skus.status LIKE '%Archived%'
                AND p.size = 2
                GROUP BY 1, 2, 3, 4, 5, 6, 7
            ) t
        GROUP BY 1,2,3
        )


    , inactiveskus_remps AS (
    SELECT market,
        unique_recipe_code,
        concat_ws(" | ", collect_set(skucode)) AS inactiveskus,
        concat_ws(" | ", collect_set(skuname)) AS inactiveskunames,
        count(DISTINCT skuname) AS inactiveskus_count
    FROM (
        SELECT r.remps_instance AS market
        , r.unique_recipe_code
        , sku.code AS skucode
        , regexp_replace(sku.display_name, '\t|\n', '') AS skuname
        , sku.status
        FROM last_recipe_remps AS r
        JOIN last_ingredient_group_remps ig
        ON r.id = ig.ingredient_group__recipe
        JOIN last_recipe_sku_remps rs
        ON ig.id = rs.recipe_sku__ingredient_group
        JOIN last_sku_remps sku
        ON sku.id = rs.recipe_sku__sku
        WHERE  rs.quantity_to_order_2p>0 AND sku.status LIKE  '%Inactive%' OR sku.status LIKE  '%Archived%'
        GROUP BY 1,2,3,4,5) t
    GROUP BY 1,2
    )




-------------------- RECIPE USAGE CTEs ---------------------




   , recipe_usage_DKSE AS (
    SELECT *
         , CASE
               WHEN last_used_running_week IS NOT  NULL AND next_used_running_week IS NOT  NULL
                   THEN next_used_running_week - last_used_running_week
               ELSE 0 END AS lastnextuseddiff
    FROM materialized_views.isa_services_recipe_usage AS r
    WHERE market = 'dkse' AND
          region_code = 'se'
    )


    , recipe_usage_CPS AS (
    SELECT * FROM materialized_views.isa_services_recipe_usage
    WHERE (region_code IN ('it','ie','es','fr') AND market IN ('it','ie','es','fr'))
        OR market = 'gb'
    )

-------------------- PICKLISTS CTEs --------------------


, picklists_CA AS (
    SELECT
    uniquerecipecode
    , concat_ws(" | ", collect_list(code)) AS skucode
    , concat_ws(" | ", collect_list(display_name)) AS skuname
    , SUM(price*quantity_to_order_2p) AS cost_2p
    , SUM(price*quantity_to_order_4p) AS cost_4p
    , count(DISTINCT code) AS skucount
    FROM (
        SELECT
        r.unique_recipe_code AS uniquerecipecode
        , sku.code
        , regexp_replace(sku.display_name, '\t|\n', '') AS display_name
        , sc.price
        , rs.quantity_to_order_2p
        , rs.quantity_to_order_4p
        FROM (SELECT * FROM last_recipe_remps WHERE remps_instance = 'CA') AS r
        JOIN (SELECT * FROM last_ingredient_group_remps WHERE remps_instance = 'CA') AS ig
            ON r.id = ig.ingredient_group__recipe
        JOIN (SELECT * FROM last_recipe_sku_remps WHERE remps_instance = 'CA') AS rs
            ON ig.id = rs.recipe_sku__ingredient_group
        LEFT JOIN materialized_views.remps_marketsetup_distributioncentres AS dc
            ON rs.recipe_sku__distribution_centre = dc.id
        JOIN (SELECT * FROM last_sku_remps WHERE remps_instance = 'CA') AS sku
            ON sku.id = rs.recipe_sku__sku
        LEFT JOIN (SELECT * FROM last_sku_cost_remps WHERE market = 'ca') AS sc
            ON sc.code=sku.code
        WHERE  rs.quantity_to_order_2p>0
        GROUP BY 1,2,3,4,5,6) t
    GROUP BY 1
)

, picklists_DACH AS (
    SELECT
    uniquerecipecode
    , concat_ws(" | ", collect_list(code)) AS skucode
    , concat_ws(" | ", collect_list(display_name)) AS skuname
    , count(DISTINCT code) AS skucount
    ,sum(quantity_to_order_2p) AS pickcount
    FROM (
        SELECT
          r.unique_recipe_code AS uniquerecipecode
        , sku.code
        , regexp_replace(sku.display_name, '\t|\n', '') AS display_name
        , rs.quantity_to_order_2p
        FROM (SELECT * FROM last_recipe_remps WHERE remps_instance = 'DACH') AS r
        JOIN (SELECT * FROM last_ingredient_group_remps WHERE remps_instance = 'DACH') AS ig
        ON r.id = ig.ingredient_group__recipe
        JOIN (SELECT * FROM last_recipe_sku_remps WHERE remps_instance = 'DACH') AS rs
        ON ig.id = rs.recipe_sku__ingredient_group
        JOIN (SELECT * FROM last_sku_remps WHERE remps_instance = 'DACH') AS sku
        ON sku.id = rs.recipe_sku__sku
        WHERE rs.quantity_to_order_2p>0
        GROUP BY 1,2,3,4) t
    GROUP BY 1
)




, picklists_DKSE AS (
        SELECT
                  unique_recipe_code
                , concat_ws(" | ", collect_list(code)) AS skucode
                , concat_ws(" | ", collect_list(NAME)) AS skuname
                , COUNT (DISTINCT code) AS skucount
                , SUM (cost2p) AS cost2p
                , SUM (cost3p) AS cost3p
                , SUM (cost4p) AS cost4p
                , size
        FROM (
            SELECT
                  r.unique_recipe_code
                , p.code
                , regexp_replace(p.name, '\t|\n', '') AS NAME
                , SUM (CASE WHEN SIZE = 2 THEN pick_count * price ELSE 0 END) AS cost2p
                , SUM (CASE WHEN SIZE = 3 THEN pick_count * price ELSE 0 END) AS cost3p
                , SUM (CASE WHEN SIZE = 4 THEN pick_count * price ELSE 0 END) AS cost4p
                , p.size
            FROM recipe_consolidated_CPS AS r
            JOIN materialized_views.culinary_services_recipe_procurement_picklist_culinarysku AS p
                ON r.id = p.recipe_id
            LEFT JOIN (SELECT * FROM sku_cost_CPS WHERE market = 'dkse' AND distribution_center = 'SK') AS C
                ON C.code = p.code
            WHERE r.market = 'dkse'
            AND p.segment_name='SE'
            GROUP BY 1, 2, 3, 7) as t
        GROUP BY 1,8
    )


, picklists_ES AS (
    SELECT
    unique_recipe_code
    , concat_ws(" | ", collect_list(code)) AS skucode
    , concat_ws(" | ", collect_list(name)) AS skuname
    , count(DISTINCT code) AS skucount
    , sum(cost1p) AS cost1p
    , sum(cost2p) AS cost2p
    , sum(cost3p) AS cost3p
    , sum(cost4p) AS cost4p
    FROM (
        SELECT
         r.unique_recipe_code
        , p.code
        , regexp_replace(p.name, '\t|\n', '') AS name
        , sum(CASE WHEN size = 1 THEN pick_count * price ELSE 0 end) AS cost1p
        , sum(CASE WHEN size = 2 THEN pick_count * price ELSE 0 end) AS cost2p
        , sum(CASE WHEN size = 3 THEN pick_count * price ELSE 0 end) AS cost3p
        , sum(CASE WHEN size = 4 THEN pick_count * price ELSE 0 end) AS cost4p
        FROM recipe_consolidated_CPS AS r
        JOIN materialized_views.culinary_services_recipe_procurement_picklist_culinarysku AS p ON r.id = p.recipe_id
        JOIN materialized_views.procurement_services_culinarysku AS pk ON p.code = pk.code
        LEFT JOIN (SELECT * FROM sku_cost_CPS WHERE market = 'es' AND distribution_center = 'SP') AS c ON c.code = p.code
        WHERE r.market = 'es'
            AND p.segment_name = 'ES'
        GROUP BY 1,2,3) t
    GROUP BY 1
)




, picklists_FR as(
    select
    unique_recipe_code
    , concat_ws(" | ", collect_list(code)) as skucode
    , concat_ws(" | ", collect_list(name)) as skuname
    , sum(cost1p) as cost1p
    , sum(cost2p) as cost2p
    , sum(cost3p) as cost3p
    , sum(cost4p) as cost4p
    from (
        select
         r.unique_recipe_code
        , p.code
        , regexp_replace(p.name, '\t|\n', '') as name
        , sum(case when size = 1 then pick_count * price else 0 end) as cost1p
        , sum(case when size = 2 then pick_count * price else 0 end) as cost2p
        , sum(case when size = 3 then pick_count * price else 0 end) as cost3p
        , sum(case when size = 4 then pick_count * price else 0 end) as cost4p
        from recipe_consolidated_CPS AS r
        join materialized_views.culinary_services_recipe_procurement_picklist_culinarysku AS p ON r.id = p.recipe_id AND r.market = p.market
        join materialized_views.procurement_services_culinarysku AS pk on p.code = pk.code and pk.market = 'beneluxfr'
        left join (SELECT * FROM sku_cost_CPS WHERE market='beneluxfr' AND distribution_center = 'DH') AS c on c.code = p.code
        where r.market = 'fr' and p.segment_name = 'FR'
            --AND p.size = 2
        group by 1,2,3) t
    group by 1
)




, picklists_GB AS (
    SELECT
    unique_recipe_code
    , concat_ws(" | ", collect_list(code)) AS skucode
    , concat_ws(" | ", collect_list(name)) AS skuname
    , sum(cost1p) AS cost1p
    , sum(cost2p) AS cost2p
    , sum(cost3p) AS cost3p
    , sum(cost4p) AS cost4p
    FROM (
        SELECT
         r.unique_recipe_code
        , p.code
        , regexp_replace(p.name, '\t|\n', '') AS name
        , sum(CASE WHEN size = 1 THEN pick_count * price ELSE 0 end) AS cost1p
        , sum(CASE WHEN size = 2 THEN pick_count * price ELSE 0 end) AS cost2p
        , sum(CASE WHEN size = 3 THEN pick_count * price ELSE 0 end) AS cost3p
        , sum(CASE WHEN size = 4 THEN pick_count * price ELSE 0 end) AS cost4p
        FROM recipe_consolidated_CPS AS r
        JOIN materialized_views.culinary_services_recipe_procurement_picklist_culinarysku AS p ON r.id = p.recipe_id AND r.market = p.market
        JOIN materialized_views.procurement_services_culinarysku AS pk ON p.code = pk.code AND p.market = pk.market
        LEFT JOIN (SELECT * FROM sku_cost_CPS WHERE market='gb' AND distribution_center = 'GR') AS c ON c.code = p.code
        WHERE r.market = 'gb' AND p.segment_name = 'GR'
        GROUP BY 1,2,3) t
    GROUP BY 1
)




, picklists_IE AS (
    SELECT market
    , segment_name
    , unique_recipe_code
    , concat_ws(" | ", collect_list(code)) AS skucode
    , concat_ws(" | ", collect_list(name)) AS skuname
    , count(DISTINCT code) AS skucount
    , SUM(cost1p) AS cost1p
    , SUM(cost2p) AS cost2p
    , SUM(cost3p) AS cost3p
    , SUM(cost4p) AS cost4p
    FROM (
        SELECT r.market
        , p.segment_name
        , r.unique_recipe_code
        , p.code
        , regexp_replace(p.name, '\t|\n', '') AS name
        , sum(CASE WHEN size = 1 THEN pick_count * price ELSE 0 end) AS cost1p
        , sum(CASE WHEN size = 2 THEN pick_count * price ELSE 0 end) AS cost2p
        , sum(CASE WHEN size = 3 THEN pick_count * price ELSE 0 end) AS cost3p
        , sum(CASE WHEN size = 4 THEN pick_count * price ELSE 0 end) AS cost4p
        FROM recipe_consolidated_CPS AS r
        JOIN materialized_views.culinary_services_recipe_procurement_picklist_culinarysku AS p ON r.id = p.recipe_id
        JOIN materialized_views.procurement_services_culinarysku AS pk ON p.code = pk.code
        LEFT JOIN (SELECT * FROM sku_cost_CPS WHERE market = 'ie' AND distribution_center = 'IE') AS c ON c.code = p.code
        WHERE r.market = 'ie' AND p.segment_name = 'IE'
        GROUP BY  1,2,3,4,5) AS t
    GROUP BY 1,2,3
)


, picklists_IT AS (
    SELECT market
    , segment_name
    , unique_recipe_code
    , concat_ws(" | ", collect_list(code)) AS skucode
    , concat_ws(" | ", collect_list(name)) AS skuname
    , COUNT(DISTINCT code) AS skucount
    , SUM(cost1p) AS cost1p
    , SUM(cost2p) AS cost2p
    , SUM(cost3p) AS cost3p
    , SUM(cost4p) AS cost4p
    FROM (
        SELECT r.market
        , p.segment_name
        , r.unique_recipe_code
        , p.code
        , regexp_replace(p.name, '\t|\n', '') AS name
        , SUM(CASE WHEN size = 1 THEN pick_count * price ELSE 0 END) AS cost1p
        , SUM(CASE WHEN size = 2 THEN pick_count * price ELSE 0 END) AS cost2p
        , SUM(CASE WHEN size = 3 THEN pick_count * price ELSE 0 END) AS cost3p
        , SUM(CASE WHEN size = 4 THEN pick_count * price ELSE 0 END) AS cost4p
        FROM recipe_consolidated_CPS AS r
        JOIN materialized_views.culinary_services_recipe_procurement_picklist_culinarysku AS p ON r.id = p.recipe_id AND r.market = p.market
        JOIN materialized_views.procurement_services_culinarysku AS pk ON p.code = pk.code AND p.market = pk.market
        LEFT JOIN (SELECT * FROM sku_cost_CPS WHERE market = 'it' AND distribution_center = 'IT') AS c ON c.code = p.code
        WHERE r.market='it'
            AND p.segment_name='IT'
        GROUP BY  1,2,3,4,5) AS t
    GROUP BY 1,2,3
)


-------------------- FILTERED RECIPES CTE --------------------




, filtered_recipes_DKSE AS (
    SELECT *
    FROM (
         SELECT r.*
                , upper(r.market) AS country
                , round(p.cost2p,2) AS cost2p
                , round(p.cost3p,2) AS cost3p
                , round(p.cost4p,2) AS cost4p
                , p.skucode
                , p.skuname
                , p.skucount
                , DENSE_RANK() OVER (PARTITION BY r.recipe_code, r.market
                                        ORDER BY r.version  DESC) AS o
        FROM recipe_consolidated_CPS AS r
        LEFT JOIN picklists_DKSE AS p
            ON p.unique_recipe_code=r.unique_recipe_code
        WHERE r.market = 'dkse'
            AND lower(r.status) NOT IN ('inactive','rejected')
            AND r.is_default = true
            AND LENGTH(r.primary_protein)>0
            AND r.primary_protein <>'N/A'
            AND p.cost2p > 0
    ) temp
    WHERE temp.o = 1
)


-------------------- ALL RECIPES CTEs --------------------


, all_recipes_CA AS (
SELECT * FROM(
SELECT cast(r.id AS string) AS uuid
       ,r.country
       ,r.uniquerecipecode
       ,r.mainrecipecode AS code
       ,r.version
       ,r.status
       ,r.title
       ,concat(r.title,coalesce (regexp_replace(r.subtitle, '\t|\n', ''),''),coalesce (r.primaryprotein,''),coalesce(r.primarystarch,''),coalesce(r.cuisine,''), coalesce(r.dishtype,''), coalesce(r.primaryvegetable,''),coalesce(r.primaryfruit,'')) AS subtitle
       ,CASE WHEN r.primaryprotein IS NULL OR r.primaryprotein = '' THEN 'not available' ELSE r.primaryprotein END AS primaryprotein
       ,TRIM(coalesce(split_part(r.primaryprotein,'-',1),r.primaryprotein)) AS mainprotein
       ,TRIM(coalesce(split_part(r.primaryprotein,'-',2),r.primaryprotein)) AS proteincut
       ,CASE WHEN r.primarystarch IS NULL OR r.primarystarch = '' THEN 'not available' ELSE r.primarystarch END AS primarystarch
       ,coalesce(TRIM(coalesce(split_part(r.primarystarch,'-',1),r.primarystarch)),'none') AS mainstarch
       ,CASE WHEN coalesce(r.primaryvegetable,'none') IS NULL OR coalesce(r.primaryvegetable,'none') = '' THEN 'not available' ELSE coalesce(r.primaryvegetable,'none') END AS primaryvegetable
       ,coalesce(TRIM(coalesce(split_part(r.primaryvegetable,'-',1),r.primaryvegetable)),'none') AS mainvegetable
       ,CASE WHEN n.fats IS NULL THEN 0 ELSE n.fats END AS fats
       ,CASE WHEN n.sugars IS NULL THEN 0 ELSE n.sugars END AS sugars
       ,CASE WHEN n.salt IS NULL THEN 0 ELSE n.salt END AS salt
       ,CASE WHEN n.kilo_calories=0 THEN 999 ELSE n.kilo_calories END AS calories
       ,CASE WHEN n.carbohydrates=0 THEN 999 ELSE n.carbohydrates END AS carbohydrates
       ,CASE WHEN n.proteins = 0 OR n.proteins IS NULL THEN 999 ELSE n.proteins END AS n_proteins
       ,CASE WHEN r.cuisine IS NULL OR r.cuisine = '' THEN 'not available' ELSE r.cuisine END AS cuisine
       ,CASE WHEN r.dishtype IS NULL OR r.dishtype = '' THEN 'not available' ELSE r.dishtype END AS dishtype
       ,CASE WHEN r.handsontime ="" OR r.handsontime IS NULL THEN 0
             when length (r.handsontime) >3 AND cast( left(r.handsontime,2) AS FLOAT) IS NULL THEN 0
             when length (r.handsontime) >3 AND cast( left(r.handsontime,2) AS FLOAT) IS NOT  NULL THEN cast( left(r.handsontime,2) AS FLOAT)
             when length (r.handsontime) <2 THEN 0
             when r.handsontime='0' THEN 0
             ELSE cast(r.handsontime AS FLOAT) END AS handsontime
       ,CASE WHEN r.totaltime ="" OR r.totaltime IS NULL THEN 0
             when length (r.totaltime) >3 AND cast( left(r.totaltime,2) AS FLOAT) IS NULL THEN 0
             when length (r.totaltime) >3 AND cast( left(r.totaltime,2) AS FLOAT) IS NOT  NULL THEN cast( left(r.totaltime,2) AS FLOAT)
             when length (r.totaltime) <2 THEN 0
             when r.totaltime='0' THEN 0
             ELSE cast(r.totaltime AS FLOAT) END AS totaltime
       ,ht.name AS hqtag
       ,rt.name AS tag
       ,CASE WHEN pf.name IS NULL OR pf.name = '' THEN 'not available' ELSE pf.name END AS preference
       ,concat (ht.name,rt.name,pf.name) AS preftag
       ,CASE WHEN pt.name IS NULL OR pt.name = '' THEN 'not available' ELSE pt.name END AS recipetype
       ,r.author
       ,p.skucode
       ,LOWER(p.skuname) AS skuname
       ,p.skucount
       --, sc2p.skucount
       , i.inactiveskus
       , i.inactiveskunames
       --,coalesce(round(p.cost_1p,4),0) as cost1p
       ,COALESCE(round(p.cost_2p,4),0) AS cost2p
       --,coalesce(round(p.cost_3p,4),0) as cost3p
       ,COALESCE(round(p.cost_4p,4),0) AS cost4p
      ,r.lastused
      ,r.absolutelastused
     --,CASE WHEN r.lastused IS NULL AND r.nextused IS NULL THEN 1 ELSE 0 END AS isnewrecipe
     --,CASE WHEN r.nextused IS NOT  NULL AND r.lastused IS NULL  THEN 1 ELSE 0 END AS isnewscheduled
     ,r.isdefault AS isdefault
     ,DENSE_RANK() OVER (PARTITION BY r.mainrecipecode, r.country, CASE WHEN right(r.uniquerecipecode,2) IN ('FR','CH','DK') THEN right(r.uniquerecipecode,2) ELSE 'X' END ORDER BY cast(r.version AS int) DESC) AS o
     ,TO_TIMESTAMP(CAST(r2.fk_imported_at AS string),'yyyyMMdd') AS updated_at --its NOT  unix timestamp
     ,CASE WHEN steps.step_description IS NULL OR steps.step_description LIKE  '% |  |  %' THEN 'not available' ELSE steps.step_description END AS step_description
     ,r.mainimageurl AS image_url
     , a.previous_allergens
     , a.current_allergens
     , a.allergen_change
     , a.allergen_updated_at
FROM (SELECT * FROM materialized_views.int_scm_analytics_remps_recipe WHERE country = 'CA') AS r
LEFT JOIN (SELECT * FROM last_recipe_remps WHERE remps_instance = 'CA') AS r2 ON r2.unique_recipe_code=r.uniquerecipecode
LEFT JOIN (SELECT * FROM last_nutrition_remps WHERE remps_instance = 'CA') AS n ON n.id=r.nutritionalinfo2p
LEFT JOIN picklists_CA AS p ON p.uniquerecipecode=r.uniquerecipecode
LEFT JOIN (SELECT * FROM preference_remps WHERE remps_instance = 'CA') AS pf ON pf.uniquerecipecode=r.uniquerecipecode
LEFT JOIN (SELECT * FROM hqtag_remps WHERE remps_instance = 'CA') AS ht ON ht.uniquerecipecode=r.uniquerecipecode
LEFT JOIN (SELECT * FROM tag_remps WHERE remps_instance = 'CA') AS rt ON rt.uniquerecipecode=r.uniquerecipecode
LEFT JOIN (SELECT * FROM producttype_remps WHERE remps_instance = 'CA') AS pt ON pt.uniquerecipecode=r.uniquerecipecode
LEFT JOIN (SELECT * FROM inactiveskus_remps WHERE market='CA') AS i ON p.uniquerecipecode = i.unique_recipe_code --and ON p.skucode = i.skucode
LEFT JOIN (SELECT * FROM steps_CPS WHERE market='ca') AS steps ON steps.unique_recipe_code = r.uniquerecipecode
LEFT JOIN allergens AS a ON r.uniquerecipecode=a.unique_recipe_code
WHERE lower(r.status) IN ('ready for menu planning','active','in development')
    AND LOWER(r.title) NOT  LIKE  '%not use%' AND LOWER(r.title) NOT LIKE '%wrong%' AND LOWER(r.title) NOT LIKE '%test%'
    AND r.uniquerecipecode NOT LIKE 'C%'
    AND r.uniquerecipecode NOT LIKE '%-FR'
    AND r.uniquerecipecode NOT LIKE 'ADD%'
    AND r.uniquerecipecode NOT LIKE 'RMOD%'
    AND r.uniquerecipecode NOT LIKE 'RCON%'
    AND r.uniquerecipecode NOT LIKE 'RAO%'
    AND r.uniquerecipecode NOT LIKE 'RRS%'
    AND r.primaryprotein IS NOT NULL
    --AND LENGTH(primaryprotein)>0
    --AND primaryprotein <>'N/A'
    AND r.country='CA'
    AND p.cost_2p >0
    AND p.cost_2p IS NOT NULL
) temp
WHERE o=1
)




, all_recipes_DACH AS (
SELECT * FROM(
SELECT cast(r.id AS string) AS uuid
       ,r.country
       ,r.uniquerecipecode
       ,r.mainrecipecode AS code
       ,r.version
       ,r.status
       ,regexp_replace(r.title, '\t|\n', '') AS title
       ,concat(r.title,coalesce(regexp_replace(r.subtitle, '\t|\n', ''),''),coalesce (r.primaryprotein,''),coalesce(r.primarystarch,''),coalesce(r.cuisine,''), coalesce(r.dishtype,'')) AS subtitle
       ,CASE WHEN r.primaryprotein IS NULL OR r.primaryprotein = '' THEN 'not available' ELSE r.primaryprotein END AS primaryprotein
       ,TRIM(coalesce(split_part(r.primaryprotein,'-',1),r.primaryprotein)) AS mainprotein
       ,TRIM(coalesce(split_part(r.primaryprotein,'-',2),r.primaryprotein)) AS proteincut
       ,CASE WHEN r.primarystarch IS NULL OR r.primarystarch = '' THEN 'not available' ELSE r.primarystarch END AS primarystarch
       ,coalesce(TRIM(coalesce(split_part(r.primarystarch,'-',1),r.primarystarch)),'none') AS mainstarch
       ,CASE WHEN coalesce(r.primaryvegetable,'none') IS NULL OR coalesce(r.primaryvegetable,'none') = '' THEN 'not available' ELSE coalesce(r.primaryvegetable,'none') END AS primaryvegetable
       ,coalesce(TRIM(coalesce(split_part(r.primaryvegetable,'-',1),r.primaryvegetable)),'none') AS mainvegetable
       ,CASE WHEN n.fats IS NULL THEN 0 ELSE n.fats END AS fats
       ,CASE WHEN n.sugars IS NULL THEN 0 ELSE n.sugars END AS sugars
       ,CASE WHEN n.salt IS NULL THEN 0 ELSE n.salt END AS salt
       ,CASE WHEN n.kilo_calories=0 THEN 999 ELSE n.kilo_calories END AS calories
       ,CASE WHEN n.carbohydrates=0 THEN 999 ELSE n.carbohydrates END AS carbohydrates
       ,CASE WHEN n.proteins = 0 OR n.proteins IS NULL THEN 999 ELSE n.proteins END AS n_proteins
       ,CASE WHEN r.cuisine IS NULL OR r.cuisine = '' THEN 'not available' ELSE r.cuisine END AS cuisine
       ,CASE WHEN r.dishtype IS NULL OR r.dishtype = '' THEN 'not available' ELSE r.dishtype END AS dishtype
       ,CASE WHEN r.handsontime ="" OR r.handsontime IS NULL THEN cast(99 AS FLOAT)
             when length (r.handsontime) >3 AND cast( left(r.handsontime,2) AS FLOAT) IS NULL THEN 99
             when length (r.handsontime) >3 AND cast( left(r.handsontime,2) AS FLOAT) IS NOT  NULL THEN cast( left(r.handsontime,2) AS FLOAT)
             when length (r.handsontime) <2 THEN cast(99 AS FLOAT)
             when r.handsontime='0' THEN cast(99 AS FLOAT)
             ELSE cast(r.handsontime AS FLOAT) END AS handsontime
       ,CASE WHEN r.totaltime ="" OR r.totaltime IS NULL THEN cast(99 AS FLOAT)
             when length (r.totaltime) >3 AND cast( left(r.totaltime,2) AS FLOAT) IS NULL THEN 99
             when length (r.totaltime) >3 AND cast( left(r.totaltime,2) AS FLOAT) IS NOT  NULL THEN cast( left(r.totaltime,2) AS FLOAT)
             when length (r.totaltime) <2 THEN cast(99 AS FLOAT)
             when r.totaltime='0' THEN cast(99 AS FLOAT)
             ELSE cast(r.totaltime AS FLOAT) END AS totaltime
      -- ,cast(right(difficultylevel,1) AS int) AS difficulty
       ,ht.name AS hqtag
       ,rt.name AS tag
       ,CASE WHEN r.uniquerecipecode LIKE  'M%' THEN 'Meister'
         when pf.name IS NULL OR pf.name = '' THEN 'not available'
         ELSE pf.name END AS preference
       ,concat (ht.name,rt.name,pf.name)  AS preftag
       ,CASE WHEN pt.name IS NULL OR pt.name = '' THEN 'not available' ELSE pt.name END AS recipetype
     ,r.author
     ,p.skucode
     ,p.skuname
     ,p.skucount
     ,i.inactiveskus
     ,i.inactiveskunames
     --,round(rc.cost_1p,2) AS cost1p
     ,round(rc.cost_2p,2) AS cost2p
     --,round(rc.cost_3p,2) AS cost3p
     ,round(rc.cost_4p,2) AS cost4p
     ,r.lastused
     --,r.nextused
     ,r.absolutelastused
     ,r.isdefault AS isdefault
     ,DENSE_RANK() OVER (PARTITION BY r.mainrecipecode, r.country, CASE WHEN right(r.uniquerecipecode,2) IN ('FR','CH','DK') THEN right(r.uniquerecipecode,2) ELSE 'X' END ORDER BY cast(r.version AS int) DESC) AS o
     ,TO_TIMESTAMP(cast(r2.fk_imported_at AS string),'yyyyMMdd') AS updated_at --its NOT  unix timestamp
     ,CASE WHEN steps.step_description IS NULL OR steps.step_description LIKE  '% |  |  %' THEN 'not available' ELSE steps.step_description END AS step_description
     ,r.mainimageurl AS image_url
     , a.previous_allergens
     , a.current_allergens
     , a.allergen_change
     , a.allergen_updated_at
FROM materialized_views.int_scm_analytics_remps_recipe AS r
LEFT JOIN (SELECT * FROM last_recipe_remps WHERE remps_instance = 'DACH') AS r2 ON r2.unique_recipe_code=r.uniquerecipecode
LEFT JOIN (SELECT * FROM last_cost_remps WHERE remps_instance = 'DACH') AS rc ON rc.id=r2.recipe__recipe_cost
LEFT JOIN (SELECT * FROM last_nutrition_remps WHERE remps_instance = 'DACH') AS n ON n.id=r.nutritionalinfo2p
LEFT JOIN picklists_DACH p ON p.uniquerecipecode=r.uniquerecipecode
LEFT JOIN (SELECT * FROM preference_remps WHERE remps_instance = 'DACH') AS pf ON pf.uniquerecipecode=r.uniquerecipecode
LEFT JOIN (SELECT * FROM hqtag_remps WHERE remps_instance = 'DACH') AS ht ON ht.uniquerecipecode=r.uniquerecipecode
LEFT JOIN (SELECT * FROM tag_remps WHERE remps_instance = 'DACH') AS rt ON rt.uniquerecipecode=r.uniquerecipecode
LEFT JOIN (SELECT * FROM producttype_remps WHERE remps_instance = 'DACH') AS pt ON pt.uniquerecipecode=r.uniquerecipecode
LEFT JOIN uploads.dach_goat_risk_complexity com ON com.uniquerecipecode=r.uniquerecipecode
LEFT JOIN (SELECT * FROM inactiveskus_remps WHERE market = 'DACH') AS i ON p.uniquerecipecode = i.unique_recipe_code --and ON p.skucode = i.skucode
LEFT JOIN (SELECT * FROM steps_CPS WHERE market='dach') AS steps ON steps.unique_recipe_code = r.uniquerecipecode
LEFT JOIN allergens AS a ON r.uniquerecipecode=a.unique_recipe_code
WHERE lower(r.status)  IN ('ready for menu planning','pool','rework')
    AND  CASE WHEN lower(r.status) IN ('ready for menu planning','rework') THEN versionused IS NOT  NULL ELSE TRUE end
    AND rc.cost_2p >1.5
    AND rc.cost_3p>0
    AND rc.cost_4p>0
    AND lower(r.title) NOT  LIKE  '%not use%' AND lower(r.title) NOT  LIKE  '%wrong%'
    --AND length (primaryprotein)>0
    --AND primaryprotein <>'N/A'
    AND  r.country='DACH'
    AND right(r.uniquerecipecode,2)<>'CH'
    AND r.uniquerecipecode NOT  LIKE  'TEST%'
    AND r.uniquerecipecode NOT  LIKE  'HE%'
    AND r.uniquerecipecode NOT  LIKE  'ADD%'
    AND r.uniquerecipecode NOT  LIKE  'CO%'
    AND r.uniquerecipecode NOT  LIKE  'XMAS%'
    AND r.title NOT  LIKE  'PLACEH%'
    /*and  CASE WHEN lower(r.status)  IN ('ready for menu planning') THEN r.absolutelastused >='2019-W01'
                ELSE TRUE  end
    AND ncat.primary_protein IS NOT  NULL*/
) temp
WHERE o=1)


, all_recipes_DKSE AS (
    SELECT
             r.id AS uuid
            , r.country
            , r.unique_recipe_code AS uniquerecipecode
            , r.recipe_code AS code
            , r.version
            , r.status
            , regexp_replace(r.title, '\t|\n', '') AS title
            , concat(regexp_replace(r.title, '\t|\n', ''), coalesce(regexp_replace(r.subtitle, '\t|\n', ''),'') ,coalesce (r.primary_protein,''),coalesce(r.primary_starch,''),coalesce(r.cuisine,''), coalesce(r.dish_type,''), coalesce(r.primary_vegetable,'')) as subtitle
            , CASE WHEN r.primary_protein IS NULL OR r.primary_protein = '' THEN 'not available' ELSE r.primary_protein END AS primaryprotein
            , r.main_protein AS mainprotein
            , r.protein_cut AS proteincut
            , CASE WHEN r.primary_starch IS NULL OR r.primary_starch = '' THEN 'not available' ELSE r.primary_starch END AS primarystarch
            , r.main_starch AS mainstarch
            , CASE WHEN coalesce(r.primary_vegetable,'none') IS NULL OR coalesce(r.primary_vegetable,'none') = '' THEN 'not available' ELSE coalesce(r.primary_vegetable,'none') END AS primaryvegetable
            , r.main_vegetable AS mainvegetable
            , CASE WHEN n.fats IS NULL THEN 0 ELSE n.fats END AS fats
            , CASE WHEN n.sugars IS NULL THEN 0 ELSE n.sugars END AS sugars
            , CASE WHEN n.salt IS NULL THEN 0 ELSE n.salt END AS salt
            , CASE WHEN n.energy = 0 OR n.energy IS NULL THEN 0 ELSE n.energy END AS calories
            , CASE WHEN n.carbs = 0 OR n.carbs IS NULL THEN 0 ELSE n.carbs END AS carbohydrates
            , CASE WHEN n.proteins = 0 OR n.proteins IS NULL THEN 0 ELSE n.proteins END AS n_proteins
            , CASE WHEN r.cuisine IS NULL OR r.cuisine = '' THEN 'not available' ELSE r.cuisine END AS cuisine
            , CASE WHEN r.dish_type IS NULL OR r.dish_type = '' THEN 'not available' ELSE r.dish_type END AS dishtype
            , CASE WHEN r.hands_on_time ='' OR r.hands_on_time IS NULL THEN cast(99 AS FLOAT)
                ELSE cast(r.hands_on_time AS FLOAT) END AS handsontime
            , CASE WHEN r.hands_on_time_max ='' OR r.hands_on_time_max IS NULL THEN cast(r.hands_on_time AS FLOAT)
                 ELSE cast(r.hands_on_time_max AS FLOAT) end
                  +
              CASE WHEN r.hands_off_time_max ='' OR r.hands_off_time_max IS NULL THEN cast(r.hands_off_time AS FLOAT)
                 ELSE cast(r.hands_off_time_max AS FLOAT) end
                  AS totaltime
            , r.tags AS hqtag --only a filler
            , r.tags AS tag
            , CASE WHEN r.target_preferences IS NULL OR r.target_preferences = '' THEN 'not available' ELSE r.target_preferences END AS preference
            , concat (r.tags,r.target_preferences) AS preftag
            --, r.target_products AS producttype
            , CASE WHEN r.recipe_type IS NULL OR r.recipe_type = '' THEN 'not available' ELSE r.recipe_type END AS recipetype
            , r.created_by AS author
            --, r.label
            , r.skucode
            ,lower(r.skuname) AS skuname
            --, p.skucount
            , sc2p.skucount
            , i.inactiveskus
            , i.inactiveskuname
            --, r.cost1p
            , r.cost2p
            --, r.cost3p
            , r.cost4p
            --, r.pricemissingskus
            --, r.boxitem
            , u.last_used AS lastused
            --, u.last_used_running_week
            --, u.next_used AS nextused
            --, u.next_used_running_week
            , CASE WHEN u.absolute_last_used IS NULL THEN '' ELSE u.absolute_last_used END AS absolutelastused
            --, CASE WHEN u.absolute_last_used_running_week IS NULL THEN -1 ELSE u.absolute_last_used_running_week END AS absolutelastusedrunning
            --, u.lastnextuseddiff
            --, coalesce(cast(u.is_newrecipe AS integer),1) AS isnewrecipe
            --, coalesce(cast(u.is_newscheduled AS integer),0) AS isnewscheduled
            , r.is_default AS isdefault
            , r.o
            , r.updated_at AS updated_at
            , CASE WHEN steps.step_description IS NULL OR steps.step_description LIKE  '% |  |  %' THEN 'not available' ELSE steps.step_description END AS step_description
            , r.image_url
            , a.previous_allergens
            , a.current_allergens
            , a.allergen_change
            , a.allergen_updated_at
            --, a.max_update
    FROM filtered_recipes_DKSE AS r
        LEFT JOIN recipe_usage_DKSE AS u
            ON u.recipe_code = r.recipe_code
        LEFT JOIN (SELECT * FROM nutrition_CPS WHERE market = 'dkse' AND segment = 'SE') AS n
            ON n.recipe_id = r.id
       -- LEFT JOIN picklists_DKSE AS p
       --     ON p.unique_recipe_code=r.unique_recipe_code
        LEFT JOIN (SELECT * FROM inactiveskus_CPS WHERE market = 'dkse' AND segment_name = 'SE' ) AS i
            ON i.unique_recipe_code = r.unique_recipe_code --and ON p.skucode = i.skucode
        LEFT JOIN (SELECT * FROM skucount_2p_CPS WHERE market = 'dkse' AND segment_name = 'SE') AS sc2p
            ON sc2p.unique_recipe_code=r.unique_recipe_code
        LEFT JOIN (SELECT * FROM steps_CPS WHERE market='dkse') AS steps ON steps.recipe_id = r.id
        LEFT JOIN allergens AS a ON r.unique_recipe_code=a.unique_recipe_code
    WHERE lower(r.status) NOT  IN ('inactive','rejected')
    ORDER BY 3
)




, all_recipes_ES AS (
SELECT * FROM(
SELECT r.id AS uuid
       ,upper(r.market) AS country
       ,r.unique_recipe_code AS uniquerecipecode
       ,r.recipe_code AS code
       ,r.version
       ,r.status
       ,regexp_replace(r.title, '\t|\n', '') AS title
       ,concat(regexp_replace(r.title, '\t|\n', ''), coalesce(regexp_replace(r.subtitle, '\t|\n', ''),'') ,coalesce (r.primary_protein,''),coalesce(r.primary_starch,''),coalesce(r.cuisine,''), coalesce(r.dish_type,''), coalesce(r.primary_vegetable,'')) AS subtitle
       ,CASE WHEN r.primary_protein IS NULL OR r.primary_protein = "" THEN 'not available' ELSE r.primary_protein END AS primaryprotein
       ,r.main_protein AS mainprotein
       ,r.protein_cut AS proteincut
       ,CASE WHEN r.primary_starch IS NULL OR r.primary_starch = '' THEN 'not available' ELSE r.primary_starch END AS primarystarch
       ,r.main_starch AS mainstarch
       ,CASE WHEN coalesce(r.primary_vegetable,'none') IS NULL OR coalesce(r.primary_vegetable,'none') = '' THEN 'not available' ELSE r.primary_protein END AS primaryvegetable
       ,r.main_vegetable AS mainvegetable
       ,CASE WHEN n.fats IS NULL THEN 0 ELSE n.fats END AS fats
       ,CASE WHEN n.sugars IS NULL THEN 0 ELSE n.sugars END AS sugars
       ,CASE WHEN n.salt IS NULL THEN 0 ELSE n.salt END AS salt
       ,CASE WHEN n.energy = 0 OR n.energy IS NULL THEN 999 ELSE n.energy END AS calories
       ,CASE WHEN n.carbs = 0  OR n.carbs IS NULL THEN 999 ELSE n.carbs END AS carbohydrates
       ,CASE WHEN n.proteins = 0 OR n.proteins IS NULL THEN 999 ELSE n.proteins END AS n_proteins
       ,CASE WHEN r.cuisine IS NULL OR r.cuisine = '' THEN 'not available' ELSE r.cuisine END AS cuisine
       ,CASE WHEN r.dish_type IS NULL OR r.dish_type = '' THEN 'not available' ELSE r.dish_type END AS dishtype
       ,CASE WHEN r.hands_on_time ="" OR r.hands_on_time IS NULL THEN cast(99 AS FLOAT)
             ELSE cast(r.hands_on_time AS FLOAT) END AS handsontime
       ,CASE WHEN r.hands_on_time_max ="" OR r.hands_on_time_max IS NULL THEN cast(r.hands_on_time AS FLOAT)
             ELSE cast(r.hands_on_time_max AS FLOAT) end
              +
        CASE WHEN r.hands_off_time_max ="" OR r.hands_off_time_max IS NULL THEN cast(r.hands_off_time AS FLOAT)
             ELSE cast(r.hands_off_time_max AS FLOAT) end
              AS totaltime
       ,r.tags AS hqtag --only a filler
       ,r.tags AS tag
       ,CASE WHEN r.target_preferences IS NULL OR r.target_preferences = '' THEN 'not available' ELSE r.target_preferences END AS preference
       ,concat (r.tags,r.target_preferences) AS preftag
       ,CASE WHEN r.recipe_type IS NULL OR r.recipe_type = '' THEN 'not available' ELSE r.recipe_type END AS recipetype
       ,r.created_by AS author
       ,p.skucode
       ,lower(p.skuname) AS skuname
       --, p.skucount
       , sc2p.skucount
       , i.inactiveskus
       , i.inactiveskuname
       --,round(p.cost1p,2) AS cost1p
       ,round(p.cost2p,2) AS cost2p
       --,round(p.cost3p,2) AS cost3p
       ,round(p.cost4p,2) AS cost4p
     ,u.last_used AS lastused
     ,CASE WHEN u.absolute_last_used IS NULL THEN '' ELSE u.absolute_last_used END AS absolutelastused
     --,coalesce(cast(u.is_newrecipe AS integer),1) AS isnewrecipe
     --,coalesce(cast(u.is_newscheduled AS integer),0) AS isnewscheduled
     ,r.is_default AS isdefault
     ,DENSE_RANK() OVER (PARTITION BY r.recipe_code, r.market ORDER BY r.version  DESC) AS o
     ,r.updated_at AS updated_at --its NOT  unix timestamp
     ,CASE WHEN steps.step_description IS NULL OR steps.step_description LIKE  '% |  |  %' THEN 'not available' ELSE steps.step_description END AS step_description
     ,r.image_url
     , a.previous_allergens
     , a.current_allergens
     , a.allergen_change
     , a.allergen_updated_at
FROM recipe_consolidated_CPS AS r
LEFT JOIN (SELECT * FROM recipe_usage_CPS WHERE region_code = 'es' AND market = 'es') AS u ON u.recipe_code = r.recipe_code
LEFT JOIN (SELECT * FROM nutrition_CPS WHERE market = 'es' AND segment = 'ES') AS n ON n.recipe_id = r.id
LEFT JOIN picklists_ES AS p ON p.unique_recipe_code=r.unique_recipe_code
LEFT JOIN (SELECT * FROM skucount_2p_CPS WHERE market = 'es' AND segment_name = 'ES') AS sc2p ON sc2p.unique_recipe_code=r.unique_recipe_code
LEFT JOIN (SELECT * FROM inactiveskus_CPS WHERE market = 'es' AND segment_name = 'ES' ) AS i ON p.unique_recipe_code = i.unique_recipe_code --and ON p.skucode = i.skucode
LEFT JOIN (SELECT * FROM steps_CPS WHERE market='es') AS steps ON steps.recipe_id = r.id
LEFT JOIN allergens AS a ON r.unique_recipe_code=a.unique_recipe_code
WHERE lower(r.status) IN ('ready for menu planning')
    AND  r.market='es'
    --AND length(r.primary_protein)>0
    --AND r.primary_protein <>'N/A'
    AND  p.cost2p >0
    AND  p.cost4p >0
) temp
WHERE isdefault=1
ORDER BY 3
)


, all_recipes_FR AS (
SELECT * FROM(
SELECT  r.id as uuid
       ,upper(r.market) AS country
       ,r.unique_recipe_code AS uniquerecipecode
       ,r.recipe_code AS code
       ,r.version
       ,r.status
       ,regexp_replace(r.title, '\t|\n', '') AS title
       ,concat(regexp_replace(r.title, '\t|\n', ''), COALESCE(regexp_replace(r.subtitle, '\t|\n', ''),'') ,COALESCE(r.primary_protein,''),COALESCE(r.primary_starch,''),COALESCE(r.cuisine,''), COALESCE(r.dish_type,''), COALESCE(r.primary_vegetable,'')) AS subtitle
       ,r.primary_protein AS primaryprotein
       ,r.main_protein AS mainprotein
       ,r.protein_cut AS proteincut
       ,r.primary_starch AS primarystarch
       ,r.main_starch AS mainstarch
       ,COALESCE(r.primary_vegetable,'none') AS primaryvegetable
       ,r.main_vegetable AS mainvegetable
       ,CASE WHEN n.fats IS NULL THEN 0 ELSE n.fats END AS fats
       ,CASE WHEN n.sugars IS NULL THEN 0 ELSE n.sugars END AS sugars
       ,CASE WHEN n.salt IS NULL THEN 0 ELSE n.salt END AS salt
       ,CASE WHEN n.energy=0 THEN 999 ELSE n.energy END AS calories
       ,CASE WHEN n.carbs=0 THEN 999 ELSE n.carbs END AS carbohydrates
       ,CASE WHEN n.proteins = 0 OR n.proteins IS NULL THEN 999 ELSE n.proteins END AS n_proteins
       ,CASE WHEN r.cuisine IS NULL OR r.cuisine = '' THEN 'not available' ELSE r.cuisine END AS cuisine
       ,CASE WHEN r.dish_type IS NULL OR r.dish_type = '' THEN 'not available' ELSE r.dish_type END AS dishtype
       ,CASE WHEN r.hands_on_time ="" OR r.hands_on_time IS NULL THEN CAST(99 AS FLOAT)
             ELSE CAST(r.hands_on_time AS FLOAT) END AS handsontime
       ,CASE WHEN r.hands_on_time_max ="" OR r.hands_on_time_max IS NULL THEN CAST(r.hands_on_time AS FLOAT)
             ELSE CAST(r.hands_on_time_max AS FLOAT) END
              +
        CASE WHEN r.hands_off_time_max ="" OR r.hands_off_time_max IS NULL THEN CAST(r.hands_off_time AS FLOAT)
             ELSE CAST(r.hands_off_time_max AS FLOAT)
         END AS totaltime
       ,r.tags AS hqtag
       ,r.tags AS tag
       ,r.target_preferences AS preference
       ,concat (r.tags,r.target_preferences) AS preftag
       ,r.recipe_type AS recipetype
       ,r.created_by AS author
       ,p.skucode
       ,p.skuname
       ,sc2p.skucount
       ,i.inactiveskus
       ,i.inactiveskusnames
       --,round(p.cost1p,2) AS cost1p
       ,round(p.cost2p,2) AS cost2p
       --,round(p.cost3p,2) AS cost3p
       ,round(p.cost4p,2) AS cost4p
       ,u.last_used AS lastused
       ,CASE WHEN u.absolute_last_used IS NULL THEN '' ELSE u.absolute_last_used END AS absolutelastused
       --,COALESCE(CAST(u.is_newrecipe AS INTEGER),1) AS isnewrecipe
       --,COALESCE(CAST(u.is_newscheduled AS INTEGER),0) AS isnewscheduled
       ,r.is_default AS isdefault
       ,DENSE_RANK() OVER (PARTITION BY r.recipe_code, r.market ORDER BY r.version DESC) AS o
       ,r.updated_at AS updated_at --its NOT  unix timestamp
       ,CASE WHEN steps.step_description IS NULL OR steps.step_description LIKE  '% |  |  %' THEN 'not available' ELSE steps.step_description END AS step_description
       ,r.image_url
       , a.previous_allergens
       , a.current_allergens
       , a.allergen_change
       , a.allergen_updated_at
       --, a.max_update
FROM recipe_consolidated_CPS AS r
LEFT JOIN (SELECT * FROM recipe_usage_CPS WHERE region_code = 'fr' AND market = 'fr') AS u ON  u.recipe_code = r.recipe_code
LEFT JOIN (SELECT * FROM nutrition_CPS WHERE market = 'fr' AND segment = 'FR') AS n ON n.recipe_id = r.id
--LEFT JOIN scores s ON s.mainrecipecode=r.mainrecipecode AND s.country=r.country
LEFT JOIN picklists_FR AS p ON p.unique_recipe_code=r.unique_recipe_code
LEFT JOIN inactiveskus_FR AS i ON p.unique_recipe_code = i.unique_recipe_code --and ON p.skucode = i.skucode
LEFT JOIN skucount_2p_FR AS sc2p ON sc2p.unique_recipe_code=r.unique_recipe_code
LEFT JOIN (SELECT * FROM steps_CPS WHERE market='fr') AS steps ON steps.unique_recipe_code = r.unique_recipe_code
LEFT JOIN allergens AS a ON r.unique_recipe_code=a.unique_recipe_code
WHERE LOWER(r.status) IN ('ready for menu planning','planned')
    AND  CASE WHEN LOWER(r.status)  IN ('ready for menu planning') THEN r.unique_recipe_code NOT LIKE'%NL%'
            ELSE TRUE  end
    AND  CASE WHEN (r.unique_recipe_code LIKE '%NL%' AND LOWER(r.tags) NOT LIKE '%demeter%') THEN u.absolute_last_used >='2021-W01'
            ELSE TRUE  end
    AND r.unique_recipe_code NOT LIKE 'K%'
    AND r.unique_recipe_code NOT LIKE 'T%'
    --AND LENGTH(r.primary_protein)>0
    --AND r.primary_protein <>'N/A'
    --AND r.primary_protein IS NOT  NULL
    AND p.cost2p >0
    AND p.cost3p >0
    AND p.cost4p >0
    AND r.is_default =1
    AND r.market='fr'
) temp
)




, all_recipes_GB AS (
    SELECT *
    FROM (
        SELECT r.id AS uuid
            , upper (r.market) as country
            , r.unique_recipe_code AS uniquerecipecode
            , r.recipe_code AS code
            , r.version
            , r.status
            , regexp_replace(r.title, '\t|\n', '') AS title
            , concat(regexp_replace(r.title, '\t|\n', ''), coalesce (regexp_replace(r.subtitle, '\t|\n', ''), ''), coalesce (r.primary_protein, ''), coalesce (r.primary_starch, ''), coalesce (r.cuisine, ''), coalesce (r.dish_type, ''), coalesce (r.primary_vegetable, '')) AS subtitle
            , CASE WHEN r.primary_protein IS NULL OR r.primary_protein = '' THEN 'not available' ELSE r.primary_protein END AS primaryprotein
            , r.main_protein AS mainprotein
            , r.protein_cut AS proteincut
            , CASE WHEN r.primary_starch IS NULL OR r.primary_protein = '' THEN 'not available' ELSE r.primary_starch END AS primarystarch
            , r.main_starch AS mainstarch
            , coalesce (r.primary_vegetable, 'none') AS primaryvegetable
            , r.main_vegetable AS mainvegetable
            , CASE WHEN n.fats IS NULL THEN 0 ELSE n.fats END AS fats
            , CASE WHEN n.sugars IS NULL THEN 0 ELSE n.sugars END AS sugars
            , CASE WHEN n.salt IS NULL THEN 0 ELSE n.salt END AS salt
            , CASE WHEN n.energy = 0 OR n.energy IS NULL THEN 999 ELSE n.energy END AS calories
            , CASE WHEN n.carbs=0 THEN 999 ELSE n.carbs END AS carbohydrates
            , CASE WHEN n.proteins = 0 OR n.proteins IS NULL THEN 999 ELSE n.proteins END AS n_proteins
            , CASE WHEN r.cuisine IS NULL OR r.cuisine = '' THEN 'not available' ELSE r.cuisine END AS cuisine
            , CASE WHEN r.dish_type IS NULL OR r.dish_type = '' THEN 'not available' ELSE r.dish_type END AS dishtype
            , CASE WHEN r.hands_on_time ="" OR r.hands_on_time IS NULL THEN cast (99 AS FLOAT)
                   ELSE cast (r.hands_on_time AS FLOAT)
                   END AS handsontime
            , CASE WHEN CAST(r.hands_on_time_max AS FLOAT)=0 THEN CAST(r.hands_on_time AS FLOAT)
                   ELSE CAST(r.hands_on_time_max AS FLOAT) END
              +
              CASE WHEN CAST(r.hands_off_time_max AS FLOAT)=0 THEN CAST(r.hands_off_time AS INT)
                  ELSE cast (r.hands_off_time_max AS FLOAT)
                  END AS totaltime
            , r.tags AS hqtag                                                                                                                     --only a filler
            , r.tags AS tag
            , CASE WHEN r.target_preferences IS NULL OR r.target_preferences = '' THEN 'not available' ELSE r.target_preferences END AS preference
            , concat (r.tags, r.target_preferences) AS preftag
            , CASE WHEN r.recipe_type IS NULL OR r.recipe_type = '' THEN 'not available' ELSE r.recipe_type END AS recipetype
            , r.created_by AS author
            , p.skucode
            , lower (p.skuname) AS skuname
            , sc2p.skucount
            , i.inactiveskus
            , i.inactiveskuname
            , round(p.cost2p, 2) AS cost2p
            , round(p.cost4p, 2) AS cost4p
            , u.last_used AS lastused
            , CASE WHEN u.absolute_last_used IS NULL THEN '' ELSE u.absolute_last_used END AS absolutelastused
            --, coalesce (cast (u.is_newrecipe AS integer), 1) AS isnewrecipe
            --, coalesce (cast (u.is_newscheduled AS integer), 0) AS isnewscheduled
            , r.is_default AS isdefault
            , DENSE_RANK() OVER (PARTITION BY r.recipe_code, r.market ORDER BY r.version DESC) AS o
            , r.updated_at AS updated_at                                                                                                          --its NOT  unix timestamp
            , CASE WHEN steps.step_description IS NULL OR steps.step_description LIKE '% |  |  %' THEN 'not available' ELSE steps.step_description END AS step_description
            , r.image_url
            , a.previous_allergens
            , a.current_allergens
            , a.allergen_change
            , a.allergen_updated_at
        FROM materialized_views.isa_services_recipe_consolidated AS r
--FROM recipe_consolidated_CPS AS r
        LEFT JOIN (SELECT * FROM recipe_usage_CPS WHERE market = 'gb') AS u ON u.recipe_code = r.recipe_code
        LEFT JOIN (SELECT * FROM nutrition_CPS WHERE market='gb' AND country='GB') AS n ON n.recipe_id = r.id
        LEFT JOIN picklists_GB AS p ON p.unique_recipe_code=r.unique_recipe_code
        LEFT JOIN (SELECT * FROM inactiveskus_CPS WHERE market = 'gb' AND segment_name = 'GR') AS i ON p.unique_recipe_code= i.unique_recipe_code --and ON p.skucode = i.skucode
        LEFT JOIN (SELECT * FROM skucount_2p_CPS WHERE market = 'gb' AND segment_name = 'GR') AS sc2p ON sc2p.unique_recipe_code=r.unique_recipe_code
        LEFT JOIN (SELECT * FROM steps_CPS WHERE market='gb') AS steps ON steps.unique_recipe_code = r.unique_recipe_code
        LEFT JOIN allergens AS a ON r.unique_recipe_code=a.unique_recipe_code
        WHERE lower (r.status) IN ('ready for menu planning', 'final cook', 'external testing', 'in development')
--AND p.cost2p >1.5
--AND p.cost3p >0
--AND p.cost4p >0
        AND LOWER (r.title) NOT LIKE '%not use%' AND lower (r.title) NOT LIKE '%wrong%' AND lower (r.title) NOT LIKE '%test%' AND lower (r.title) NOT LIKE '%brexit%'
        AND r.primary_protein <>'White Fish - Coley'
--AND length (r.primary_protein)>0
--AND r.primary_protein <>'N/A'
        AND UPPER(r.unique_recipe_code) NOT LIKE '%MOD%'
        AND UPPER(r.unique_recipe_code) NOT LIKE 'GC%'
        AND LOWER(r.unique_recipe_code) NOT LIKE '%test%'
        AND UPPER(r.unique_recipe_code) NOT LIKE '%ASD%'
        AND UPPER(r.unique_recipe_code) NOT LIKE 'MK%'
        AND UPPER(r.unique_recipe_code) NOT LIKE 'A%'
        AND UPPER(r.unique_recipe_code) NOT LIKE 'X%'
        AND UPPER(r.unique_recipe_code) NOT LIKE 'BUND%'
        AND r.target_products NOT IN ('add-on', 'Baking kits', 'Breakfast', 'Sides', 'Dessert', 'Bread', 'Brunch', 'Cheese', 'Desserts', 'Modularity', 'Ready Meals', 'Speedy lunch', 'Speedy Lunch', 'Soup')
        AND r.market='gb'
--and r.is_default=true
        ) temp
--WHERE o=1)
        )


, all_recipes_IE AS (
SELECT * FROM(
SELECT r.id AS uuid
       ,upper(r.market) AS country
       ,r.unique_recipe_code AS uniquerecipecode
       ,r.recipe_code AS code
       ,r.version
       ,r.status
       ,regexp_replace(r.title, '\t|\n', '') AS title
       ,concat(regexp_replace(r.title, '\t|\n', ''), coalesce(regexp_replace(r.subtitle, '\t|\n', ''),'') ,coalesce (r.primary_protein,''),coalesce(r.primary_starch,''),coalesce(r.cuisine,''), coalesce(r.dish_type,''), coalesce(r.primary_vegetable,'')) AS subtitle
       ,CASE WHEN r.primary_protein IS NULL OR r.primary_protein = "" THEN 'not available' ELSE r.primary_protein END AS primaryprotein
       ,r.main_protein AS mainprotein
       ,r.protein_cut AS proteincut
       ,CASE WHEN r.primary_starch IS NULL OR r.primary_starch = '' THEN 'not available' ELSE r.primary_starch END AS primarystarch
       ,r.main_starch AS mainstarch
       ,CASE WHEN coalesce(r.primary_vegetable,'none') IS NULL OR coalesce(r.primary_vegetable,'none') = '' THEN 'not available' ELSE r.primary_protein END AS primaryvegetable
       ,r.main_vegetable AS mainvegetable
       ,CASE WHEN n.fats IS NULL THEN 0 ELSE n.fats END AS fats
       ,CASE WHEN n.sugars IS NULL THEN 0 ELSE n.sugars END AS sugars
       ,CASE WHEN n.salt IS NULL THEN 0 ELSE n.salt END AS salt
       ,CASE WHEN n.energy = 0 OR n.energy IS NULL THEN 999 ELSE n.energy END AS calories
       ,CASE WHEN n.carbs = 0  OR n.carbs IS NULL THEN 999 ELSE n.carbs END AS carbohydrates
       ,CASE WHEN n.proteins = 0 OR n.proteins IS NULL THEN 999 ELSE n.proteins END AS n_proteins
       ,CASE WHEN r.cuisine IS NULL OR r.cuisine = '' THEN 'not available' ELSE r.cuisine END AS cuisine
       ,CASE WHEN r.dish_type IS NULL OR r.dish_type = '' THEN 'not available' ELSE r.dish_type END AS dishtype
       ,CASE WHEN r.hands_on_time ="" OR r.hands_on_time IS NULL THEN cast(99 AS FLOAT)
             ELSE cast(r.hands_on_time AS FLOAT) END AS handsontime
       ,CASE WHEN r.hands_on_time_max ="" OR r.hands_on_time_max IS NULL THEN cast(r.hands_on_time AS FLOAT)
             ELSE cast(r.hands_on_time_max AS FLOAT) end
              +
        CASE WHEN r.hands_off_time_max ="" OR r.hands_off_time_max IS NULL THEN cast(r.hands_off_time AS FLOAT)
             ELSE cast(r.hands_off_time_max AS FLOAT) end
              AS totaltime
       ,r.tags AS hqtag --only a filler
       ,r.tags AS tag
       ,CASE WHEN r.target_preferences IS NULL OR r.target_preferences = '' THEN 'not available' ELSE r.target_preferences END AS preference
       ,concat (r.tags,r.target_preferences) AS preftag
       ,CASE WHEN r.recipe_type IS NULL OR r.recipe_type = '' THEN 'not available' ELSE r.recipe_type END AS recipetype
       ,r.created_by AS author
       ,p.skucode
       ,lower(p.skuname) AS skuname
       , sc2p.skucount
       , i.inactiveskus
       , i.inactiveskuname
       --,round(p.cost1p,2) AS cost1p
       ,round(p.cost2p,2) AS cost2p
       --,round(p.cost3p,2) AS cost3p
       ,round(p.cost4p,2) AS cost4p
     ,u.last_used AS lastused
     ,CASE WHEN u.absolute_last_used IS NULL THEN '' ELSE u.absolute_last_used END AS absolutelastused
     --,coalesce(cast(u.is_newrecipe AS integer),1) AS isnewrecipe
     --,coalesce(cast(u.is_newscheduled AS integer),0) AS isnewscheduled
     ,r.is_default AS isdefault
     ,DENSE_RANK() OVER (PARTITION BY r.recipe_code, r.market ORDER BY r.version  DESC) AS o
     ,r.updated_at AS updated_at --its NOT  unix timestamp
     ,CASE WHEN steps.step_description IS NULL OR steps.step_description LIKE  '% |  |  %' THEN 'not available' ELSE steps.step_description END AS step_description
     ,r.image_url
     , a.previous_allergens
     , a.current_allergens
     , a.allergen_change
     , a.allergen_updated_at
FROM recipe_consolidated_CPS AS r
LEFT JOIN (SELECT * FROM recipe_usage_CPS WHERE region_code = 'ie' AND market = 'ie') AS u ON u.recipe_code = r.recipe_code
LEFT JOIN (SELECT * FROM nutrition_CPS WHERE market = 'ie' AND segment = 'IE') AS n ON n.recipe_id = r.id
LEFT JOIN picklists_IE AS p ON p.unique_recipe_code=r.unique_recipe_code
LEFT JOIN (SELECT * FROM skucount_2p_CPS WHERE market = 'ie' AND segment_name = 'IE') AS sc2p ON sc2p.unique_recipe_code=r.unique_recipe_code
LEFT JOIN (SELECT * FROM inactiveskus_CPS WHERE market = 'ie' AND segment_name = 'IE' ) AS i ON p.unique_recipe_code = i.unique_recipe_code --and ON p.skucode = i.skucode
LEFT JOIN (SELECT * FROM steps_CPS WHERE market='ie') AS steps ON steps.recipe_id = r.id
LEFT JOIN allergens AS a ON r.unique_recipe_code=a.unique_recipe_code
WHERE lower(r.status) IN ('ready for menu planning', 'in development')
    AND  r.market='ie'
    --AND length(r.primary_protein)>0
    --AND r.primary_protein <>'N/A'
    AND p.cost2p >0
    AND p.cost4p >0
) temp
WHERE isdefault=1)


, all_recipes_IT AS (
SELECT * FROM(
SELECT r.id AS uuid
       ,upper(r.market) AS country
       ,r.unique_recipe_code AS uniquerecipecode
       ,r.recipe_code AS code
       ,r.version
       ,r.status
       ,regexp_replace(r.title, '\t|\n', '') AS title
       ,concat(regexp_replace(r.title, '\t|\n', ''), coalesce(regexp_replace(r.subtitle, '\t|\n', ''),'') ,coalesce (r.primary_protein,''),coalesce(r.primary_starch,''),coalesce(r.cuisine,''), coalesce(r.dish_type,''), coalesce(r.primary_vegetable,'')) AS subtitle
       ,CASE WHEN r.primary_protein IS NULL OR r.primary_protein = "" THEN 'not available' ELSE r.primary_protein END AS primaryprotein
       ,r.main_protein AS mainprotein
       ,r.protein_cut AS proteincut
       ,CASE WHEN r.primary_starch IS NULL OR r.primary_starch = '' THEN 'not available' ELSE r.primary_starch END AS primarystarch
       ,r.main_starch AS mainstarch
       ,CASE WHEN coalesce(r.primary_vegetable,'none') IS NULL OR coalesce(r.primary_vegetable,'none') = '' THEN 'not available' ELSE r.primary_vegetable END AS primaryvegetable
       ,r.main_vegetable AS mainvegetable
       ,CASE WHEN n.fats IS NULL THEN 0 ELSE n.fats END AS fats
       ,CASE WHEN n.sugars IS NULL THEN 0 ELSE n.sugars END AS sugars
       ,CASE WHEN n.salt IS NULL THEN 0 ELSE n.salt END AS salt
       ,CASE WHEN n.energy = 0 OR n.energy IS NULL THEN 0 ELSE n.energy END AS calories
       ,CASE WHEN n.carbs = 0  OR n.carbs IS NULL THEN 0 ELSE n.carbs END AS carbohydrates
       ,CASE WHEN n.proteins = 0 OR n.proteins IS NULL THEN 0 ELSE n.proteins END AS n_proteins
       ,CASE WHEN r.cuisine IS NULL OR r.cuisine = '' THEN 'not available' ELSE r.cuisine END AS cuisine
       ,CASE WHEN r.dish_type IS NULL OR r.dish_type = '' THEN 'not available' ELSE r.dish_type END AS dishtype
       ,CASE WHEN r.hands_on_time_max ="" OR r.hands_on_time_max IS NULL THEN cast(99 AS FLOAT)
             ELSE cast(r.hands_on_time_max AS FLOAT) END AS handsontime
       ,CASE WHEN r.hands_on_time_max ="" OR r.hands_on_time_max IS NULL THEN cast(99 AS FLOAT)
             ELSE cast(r.hands_on_time_max AS FLOAT) END
              +
        CASE WHEN r.hands_off_time_max ="" OR r.hands_off_time_max IS NULL THEN cast(99 AS FLOAT)
             ELSE cast(r.hands_off_time_max AS FLOAT) END AS totaltime
       ,r.tags AS hqtag --only a filler
       ,r.tags AS tag
       ,CASE WHEN r.target_preferences IS NULL OR r.target_preferences = '' THEN 'not available' ELSE r.target_preferences END AS preference
       ,concat (r.tags,r.target_preferences) AS preftag
       ,CASE WHEN r.recipe_type IS NULL OR r.recipe_type = '' THEN 'not available' ELSE r.recipe_type END AS recipetype
       ,r.created_by AS author
       ,p.skucode
       ,lower(p.skuname) AS skuname
       , p.skucount
       --, sc2p.skucount
       , i.inactiveskus
       , i.inactiveskuname
       ,round(p.cost2p,2) AS cost2p
       ,round(p.cost4p,2) AS cost4p
     ,u.last_used AS lastused
     ,CASE WHEN u.absolute_last_used IS NULL THEN '' ELSE u.absolute_last_used END AS absolutelastused
     --,COALESCE(CAST(u.is_newrecipe AS integer),1) AS isnewrecipe
     --,COALESCE(CAST(u.is_newscheduled AS integer),0) AS isnewscheduled
     ,r.is_default AS isdefault
     ,DENSE_RANK() OVER (PARTITION BY r.recipe_code, r.market ORDER BY r.version  DESC) AS o
     ,r.updated_at AS updated_at --its NOT  unix timestamp
     ,CASE WHEN steps.step_description IS NULL OR steps.step_description LIKE  '% |  |  %' THEN 'not available' ELSE steps.step_description END AS step_description
     ,r.image_url
     , a.previous_allergens
     , a.current_allergens
     , a.allergen_change
     , a.allergen_updated_at
FROM recipe_consolidated_CPS AS r
LEFT JOIN (SELECT * FROM recipe_usage_CPS WHERE region_code = 'it' AND market = 'it') AS u ON u.recipe_code = r.recipe_code
LEFT JOIN (SELECT * FROM nutrition_CPS WHERE market = 'it' AND segment = 'IT') AS n ON n.recipe_id = r.id
LEFT JOIN picklists_IT AS p ON p.unique_recipe_code=r.unique_recipe_code
LEFT JOIN (SELECT * FROM inactiveskus_CPS WHERE market = 'it' AND segment_name = 'IT') AS i ON p.unique_recipe_code = i.unique_recipe_code --and ON p.skucode = i.skucode
LEFT JOIN (SELECT * FROM steps_CPS WHERE market='it') AS steps ON steps.recipe_id = r.id
LEFT JOIN allergens AS a ON r.unique_recipe_code=a.unique_recipe_code
WHERE LOWER(r.status) IN ('ready for menu planning', 'in development')
    AND r.market='it'
    --AND LENGTH(r.primary_protein)>0
    --AND r.primary_protein <>'N/A'
    AND p.cost2p >0
    AND p.cost4p >0
    AND LOWER(r.recipe_type) <> 'add-ons'
) temp
WHERE isdefault = 1
)


SELECT DISTINCT * FROM all_recipes_CA
UNION ALL
SELECT DISTINCT * FROM all_recipes_DACH
UNION ALL
SELECT DISTINCT * FROM all_recipes_DKSE
UNION ALL
SELECT DISTINCT * FROM all_recipes_ES
UNION ALL
SELECT DISTINCT * FROM all_recipes_FR
UNION ALL
SELECT DISTINCT * FROM all_recipes_GB
UNION ALL
SELECT DISTINCT * FROM all_recipes_IE
UNION ALL
SELECT DISTINCT * FROM all_recipes_IT

