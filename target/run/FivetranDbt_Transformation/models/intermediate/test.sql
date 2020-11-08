
  create or replace  view FIVETRAN_DATABASE.dbt_dataflo_staging.test  as (
    WITH source AS (

    select * from salesforce.period 

 )

 select cast(GETDATE() as date) as SYSDATE, * from source where cast(GETDATE() as date) between start_date and end_date and type='Quarter'
  );
