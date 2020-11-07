{% if not var("enable_crm_warehouse") and not enable_finance_warehouse and not enable_marketing_warehouse and not enable_projects_warehouse %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with account_user as (

    SELECT * FROM {{ ref('inter_sf_account_user') }}
)

select * from account_user
