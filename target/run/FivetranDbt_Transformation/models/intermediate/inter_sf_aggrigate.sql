
  create or replace  view FIVETRAN_DATABASE.dbt_dataflo_staging.inter_sf_aggrigate  as (
    --Fact table for DataFlo aggrigate metrics

WITH opportunity AS (

    select * from  FIVETRAN_DATABASE.dbt_dataflo_staging.stg_sf_opportunity    

), salesforce_user as (

    select * from FIVETRAN_DATABASE.dbt_dataflo_staging.stg_sf_user
  
),account as (

    select * from FIVETRAN_DATABASE.dbt_dataflo_staging.stg_sf_account
  
),created_this_month_quarter AS(
    select
      opportunity_owner_id,
      opportunity_manager.user_id as opportunity_manager_id,
      account.account_id as account_id,
      opportunity.amount,
      opportunity.days_to_close,
      probability,
      days_since_created,
      case
        when opportunity.is_won then 'Won'
        when not opportunity.is_won and opportunity.is_closed then 'Lost'
        when not opportunity.is_closed and lower(opportunity.forecast_category) in ('pipeline','forecast','bestcase') then 'Pipeline'
        else 'Other'
      end as status,
      month(close_date) as closed_month,
      case when is_created_this_month then amount else 0 end as created_amount_this_month,
      case when is_created_this_quarter then amount else 0 end as created_amount_this_quarter,
      case when is_created_this_month then 1 else 0 end as created_count_this_month,
      case when is_created_this_quarter then 1 else 0 end as created_count_this_quarter,
      case when is_closed_this_month then amount else 0 end as closed_amount_this_month,
      case when is_closed_this_quarter then amount else 0 end as closed_amount_this_quarter,
      case when is_closed_this_month then 1 else 0 end as closed_count_this_month,
      case when is_closed_this_quarter then 1 else 0 end as closed_count_this_quarter

    from opportunity  
    left join account on opportunity.opportunity_account_id = account.account_id
    left join salesforce_user as opportunity_owner on opportunity.opportunity_owner_id = opportunity_owner.user_id
    left join salesforce_user as opportunity_manager on opportunity_owner.manager_id = opportunity_manager.user_id
    where opportunity.opportunity_owner_id is not null

), won_by_owner as (

  select 
    opportunity_manager_id as won_manager_id,
    opportunity_owner_id as won_owner_id,
    round(sum(closed_amount_this_month)) as won_amount_closed_this_month,
    round(sum(closed_amount_this_quarter)) as won_amount_closed_this_quarter,
    count(*) as total_number_won,
    round(sum(amount)) as total_won_amount,
    round(avg(amount)) as avg_won_amount,
    max(amount) as max_won_amount,
    min(amount) as min_won_amount,
    sum(closed_count_this_month) as won_count_closed_this_month,
    sum(closed_count_this_quarter) as won_count_closed_this_quarter,
    avg(days_to_close) as avg_days_to_close_won
  from created_this_month_quarter
  where status = 'Won' and opportunity_owner_id is not null
  group by 1, 2

), lost_by_owner as (

  select 
    opportunity_manager_id as lost_manager_id,
    opportunity_owner_id as lost_owner_id,
    round(sum(closed_amount_this_month)) as lost_amount_this_month,
    round(sum(closed_amount_this_quarter)) as lost_amount_this_quarter,
    count(*) as total_number_lost,
    round(sum(amount)) as total_lost_amount,
    max(amount) as max_lost_amount,
    min(amount) as min_lost_amount,
    sum(closed_count_this_month) as lost_count_this_month,
    sum(closed_count_this_quarter) as lost_count_this_quarter
  from created_this_month_quarter
  where status = 'Lost' and opportunity_owner_id is not null
  group by 1, 2

), pipeline_by_owner as (

  select 
    opportunity_manager_id as p_manager_id,
    opportunity_owner_id as p_owner_id,
    round(sum(created_amount_this_month)) as pipeline_created_amount_this_month,
    round(sum(created_amount_this_quarter)) as pipeline_created_amount_this_quarter,
    round(sum(created_amount_this_month * probability)) as pipeline_created_forecast_amount_this_month,
    round(sum(created_amount_this_quarter * probability)) as pipeline_created_forecast_amount_this_quarter,
    sum(created_count_this_month) as pipeline_count_created_this_month,
    sum(created_count_this_quarter) as pipeline_count_created_this_quarter,
    count(*) as total_number_pipeline,
    round(sum(amount)) as total_pipeline_amount,
    round(sum(amount * probability)) as total_pipeline_forecast_amount,
    round(avg(amount)) as avg_pipeline_opp_amount,
    max(amount) as largest_deal_in_pipeline,
    avg(days_since_created) as avg_days_open
  from created_this_month_quarter
  where status = 'Pipeline' and opportunity_owner_id is not null
  group by 1, 2
)

select 
  --salesforce_user.user_id as owner_id,
  coalesce(won_owner_id, lost_owner_id, p_owner_id) as opp_owner_id,
  coalesce(p_manager_id, won_manager_id, lost_manager_id) as manager_id,
  won_by_owner.*,
  lost_by_owner.*,
  pipeline_by_owner.*
from salesforce_user
left join won_by_owner on won_by_owner.won_owner_id = salesforce_user.user_id
left join lost_by_owner on lost_by_owner.lost_owner_id = salesforce_user.user_id
left join pipeline_by_owner on pipeline_by_owner.p_owner_id = salesforce_user.user_id

where won_owner_id is not null
  );
