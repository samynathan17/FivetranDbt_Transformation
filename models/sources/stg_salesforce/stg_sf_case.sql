{% if not var("enable_salesforce_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

{% if var("stg_salesforce_crm_etl") == 'fivetran' %}

 with source as (

  select * from {{ var('stg_salesforce_schema') }}.{{ var('stg_salesforce_case_table')}}

  )
{% endif %}  

select * from source 
  