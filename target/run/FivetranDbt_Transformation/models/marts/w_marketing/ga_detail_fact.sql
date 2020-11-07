

      create or replace transient table FIVETRAN_DATABASE.dbt_dataflo.ga_detail_fact  as
      (select * from FIVETRAN_DATABASE.dbt_dataflo_staging.inter_detail_ga
      );
    