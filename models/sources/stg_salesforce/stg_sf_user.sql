{% if not var("enable_salesforce_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

{% if var("stg_salesforce_crm_etl") == 'fivetran' %}
    WITH source AS (

    select * from {{ var('stg_salesforce_schema') }}.{{ var('stg_salesforce_user_table') }}

    ),renamed as (
        select 
          id as user_id,
          username,
          name,
          city,
          state,
          manager_id,
          user_role_id,
          company_name,
          email,
          user_type,
          employee_number,
          TIME_ZONE_SID_KEY,
          case
          when TIME_ZONE_SID_KEY='America/Los_Angeles' then '$' 
          when TIME_ZONE_SID_KEY='India' then 'INR' else null end as rupeesympol,
          CREATED_DATE,
          CREATED_BY_ID,
          LAST_MODIFIED_DATE,
          LAST_MODIFIED_BY_ID,
          _FIVETRAN_DELETED as IS_DELETED
        from source 
        )

{% endif %}

select * from renamed
