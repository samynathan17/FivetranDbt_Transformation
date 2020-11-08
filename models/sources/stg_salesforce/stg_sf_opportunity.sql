{% if not var("enable_salesforce_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}


{% if var("stg_salesforce_crm_etl") == 'fivetran' %}
    WITH source AS (

    select * from {{ var('stg_salesforce_schema') }}.{{ var('stg_salesforce_opportunity_table') }}

    ),renamed as (
    select
      id as opportunity_id,

        -- keys
        account_id as opportunity_account_id,
        owner_id as opportunity_owner_id,

        -- company context
        name as company_name,
        description as opportunity_description,
        type as opportunity_type,

        -- attribution
        lead_source,
        is_private,

        -- pipeline
        fiscal as fiscal_yq,
        fiscal_year,
        fiscal_quarter,
        probability,
        next_step,
        has_open_activity,
        stage_name,
        forecast_category,
        forecast_category_name,
        campaign_id,
        is_won,
        is_closed,
        close_date,
        has_overdue_task,
        expected_revenue,
        total_opportunity_quantity,

        delivery_installation_status_c as delivery_installation_status,
        order_number_c as order_number,
        current_generators_c as current_generators,
        main_competitors_c as main_competitors,

        -- accounting
        amount,

        -- metadata
        last_activity_date,
        created_date as created_at,
        last_modified_date as updated_at,
        IS_DELETED,
        DATEADD(DD,-7,GETDATE()) AS lastweek,
        DATEADD(DD,0,GETDATE()) AS thisweek,
        DATEADD(MM,-1,GETDATE()) AS lastmonth,
        DATEADD(MM,0,GETDATE()) AS thismonth,
        extract(quarter from date_trunc('quarter', GETDATE())::date - 1) AS lastquarter,
        CURRENT_DATE() AS thisquarter,
        DATEADD(year,-1,GETDATE()) AS lastyear,
        DATEADD(year,0,GETDATE()) AS thisyear,
        created_date >= {{ dbt_utils.date_trunc('month', dbt_utils.current_timestamp()) }} as is_created_this_month,
        created_date >= {{ dbt_utils.date_trunc('quarter', dbt_utils.current_timestamp()) }} as is_created_this_quarter,
        {{ dbt_utils.datediff(dbt_utils.current_timestamp(), 'created_date', 'day') }} as days_since_created,
        {{ dbt_utils.datediff('close_date', 'created_date', 'day') }} as days_to_close,
        {{ dbt_utils.date_trunc('month', 'close_date') }} = {{ dbt_utils.date_trunc('month', dbt_utils.current_timestamp()) }} as is_closed_this_month,
        {{ dbt_utils.date_trunc('quarter', 'close_date') }} = {{ dbt_utils.date_trunc('quarter', dbt_utils.current_timestamp()) }} as is_closed_this_quarter


    from source
)
{% endif %}

select * from renamed
