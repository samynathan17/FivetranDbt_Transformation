

      create or replace transient table FIVETRAN_DATABASE.dbt_dataflo.dim_sf_opportunity  as
      (

WITH opportunity_lead as (

 select * from  FIVETRAN_DATABASE.dbt_dataflo_staging.inter_sf_opportunity_leads
)    
select * from opportunity_lead
      );
    