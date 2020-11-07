
{% if not var("enable_salesforce_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

{% if var("stg_salesforce_crm_etl") == 'fivetran' %}
    WITH source AS (

    select * from {{ var('stg_salesforce_schema') }}.{{ var('stg_salesforce_user_role_table') }}

    ), renamed as (

    select 

      id as user_role_id,
      name as role_name,
      developer_name,
      _fivetran_deleted as IS_DELETED

    from source
    )
{% endif %}

select * from renamed
