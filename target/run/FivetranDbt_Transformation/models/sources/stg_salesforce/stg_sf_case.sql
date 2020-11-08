
  create or replace  view FIVETRAN_DATABASE.dbt_dataflo_staging.stg_sf_case  as (
    



 with source as (

  select * from salesforce.case

  )
  

select * from source
  );
