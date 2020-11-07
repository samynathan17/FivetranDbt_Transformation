
  create or replace  view FIVETRAN_DATABASE.dbt_dataflo_staging.sdfc_account  as (
    with source as (

    SELECT *
    FROM FIVETRAN_DATABASE.dbt_dataflo_staging.stg_sf_account
    WHERE account_id IS NOT NULL
    

)

SELECT *
FROM source
  );
