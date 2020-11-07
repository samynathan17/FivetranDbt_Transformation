--Dimension table for lead & opportunity join 
WITH leads as (
        select * from  {{ ref('stg_sf_lead') }}
    ),opport as (
        select * from  {{ ref('stg_sf_opportunity') }}    
    ),opportunity_lead as (

     select
        leads.*,
        opportunity_id,
        opportunity_owner_id,
        opportunity_type,
        fiscal_yq,
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
        delivery_installation_status,
        order_number,
        current_generators,
        main_competitors,
        -- accounting
        amount,
        case
        when opportunity.is_won then 'Won'
        when not opportunity.is_won and opportunity.is_closed then 'Lost'
        when not opportunity.is_closed and lower(opportunity.forecast_category) in ('pipeline','forecast','bestcase') then 'Pipeline'
        else 'Other' end as opportunity_status,
        
        created_at as opportunity_created_at,
        updated_at as opportunity_updated_at

      from opport as opportunity
        LEFT JOIN 
        (select 
            lead_owner_id,lead_id,
            lead_source,
            status as lead_status ,
            full_name as lead_fullname,
            company_name as lead_companyname
       from leads ) as leads
        on opportunity.opportunity_owner_id = leads.lead_owner_id

    )

select * from opportunity_lead
