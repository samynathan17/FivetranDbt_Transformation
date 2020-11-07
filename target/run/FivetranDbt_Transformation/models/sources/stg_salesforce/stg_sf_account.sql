
  create or replace  view FIVETRAN_DATABASE.dbt_dataflo_staging.stg_sf_account  as (
    


    WITH source AS (

    select * from salesforce.account 

    ),renamed as (
      select
      
        id as account_id,
        name as account_name,
        -- keys
        parent_id,
        owner_id as account_owner_id,

        -- logistics
        type as account_type,
        billing_street as company_street,
        billing_city as company_city,
        billing_state as company_state,
        billing_country as company_country,
        billing_postal_code as company_zipcode,

        -- Shipping details
        shipping_street,
        shipping_city,
        shipping_state,
        shipping_country,
        shipping_postal_code as shipping_zipcode,
        account_number,
        -- details
        industry,
        description,
        number_of_employees as number_of_employees,
        annual_revenue,
        ownership,

        -- metadata
        last_activity_date,
        created_date as created_at,
        last_modified_date as updated_at,
        clean_status,
        account_source,
        customer_priority_c as customer_priority,
        IS_DELETED

      from source
    )


select * from renamed
  );
