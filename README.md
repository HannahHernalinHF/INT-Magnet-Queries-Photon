# INT-Magnet-Queries-Photon
The queries involved extracting the data in Databricks Photon for the INT Magnet Dashboard. 
Link to Dashboard: https://tableau.hellofresh.io/#/views/INTMagnet/Home?:iid=1

1. INTMagnet_RecipePool_Query_db.sql
Executed In: Custom SQL Query in Tableau Prep Builder connected to Databricks Photon.
This query is used to extract the recipe pool data of 6 CDP and 2 REMPS markets including the ff:
     CDP markets: DKSE, ES, FR, GB, IE, IT
     REMPS markets: CA, DACH
Columns: {uuid,country,uniquerecipecode,code,version,status,title,subtitle,primaryprotein,mainprotein,proteincut,primarystarch,mainstarch,primaryvegetable,mainvegetable,fats,sugars,salt,calories,carbohydrates,n_proteins,cuisine,dishtype,handsontime,totaltime,hqtag,tag,preference,preftag,recipetype,author,skucode,skuname,skucount,inactiveskus,inactiveskunames,cost2p,cost4p,lastused,absolutelastused,isdefault,o,updated_at,step_description,image_url,previous_allergens,current_allergens,allergen_change,allergen_updated_at}

2. MissingStaticPrice_Query_db.sql
Executed In: Custom SQL Query in Tableau Prep Builder connected to Databricks Photon.
This query extracts the recipes with missing SKUs' static prices for the 'static-price' error.
Columns:
{market,hellofresh_week,recipe_status,unique_recipe_code,title,is_default,sku_status,price,sku_code,sku_name}

3. ANZ_InactiveSKUs_Query_db.sql
Executed In: Databricks Photon
This query is used to extract the inactive SKUs of the 3 ANZ markets:
     a. HelloFresh - AU
     b. HelloFresh - NZ
     c. EveryPlate - AU
*** for the purpose of the 'inactive-sku' error.

Columns:
  a. iskucodes - stands for inactive SKU codes
  b. iskunames - stands for inactive SKU codes
  c. skustatus - stands for inactive SKU codes

The data are then uploaded to the InactiveSKU tab of the ANZ Data Google Sheet File:
Link to ANZ Data: https://docs.google.com/spreadsheets/d/1BSXvGA1W507eNBn14ZJjiwY-5eUC2CQ7b7bVkRPm6zY/edit#gid=2146979532
