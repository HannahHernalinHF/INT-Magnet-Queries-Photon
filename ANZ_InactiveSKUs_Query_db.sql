----- ANZ Inactive SKUs -----

SELECT DISTINCT sku_code as iskucodes
    , sku_name as iskunames
    , sku_status as skustatus
FROM uploads.gp_octopus_sku_details
WHERE sku_status = 'Inactive'

