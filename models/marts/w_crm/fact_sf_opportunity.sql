{% if not var("enable_crm_warehouse") and not enable_finance_warehouse and not enable_marketing_warehouse and not enable_projects_warehouse %}
{{
    config(
        enabled=false
    )
}}
{% endif %}


select * from  {{ ref('inter_sf_opportunity_enhanced') }}

--  LEFT JOIN 
--         (select * from opportunity_account_dim ) as account_user
--         on opportunity_fact.OPPORTUNITY_OWNER_ID = account_user.account_owner_id



