

      create or replace transient table FIVETRAN_DATABASE.dbt_dataflo.dim_opportunity_account  as
      (

with account_user as (

    SELECT * FROM FIVETRAN_DATABASE.dbt_dataflo_staging.inter_sf_account_user
)

select * from account_user
      );
    