

      create or replace transient table FIVETRAN_DATABASE.dbt_dataflo.fact_sf_opportunity  as
      (


select * from  FIVETRAN_DATABASE.dbt_dataflo_staging.inter_sf_opportunity_enhanced

--  LEFT JOIN 
--         (select * from opportunity_account_dim ) as account_user
--         on opportunity_fact.OPPORTUNITY_OWNER_ID = account_user.account_owner_id
      );
    