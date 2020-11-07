{% if not var("enable_crm_warehouse") and not enable_finance_warehouse and not enable_marketing_warehouse and not enable_projects_warehouse %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH opportunity_lead as (

 select * from  {{ ref('inter_sf_opportunity_leads') }}
)    
select * from opportunity_lead
