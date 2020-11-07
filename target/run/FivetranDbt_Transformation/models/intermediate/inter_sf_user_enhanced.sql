
  create or replace  view FIVETRAN_DATABASE.dbt_dataflo_staging.inter_sf_user_enhanced  as (
    WITH account AS (

    select * from  FIVETRAN_DATABASE.dbt_dataflo_staging.stg_sf_account

    ),users as (
        select * from  FIVETRAN_DATABASE.dbt_dataflo_staging.stg_sf_user
    ),contact as (
        select * from  FIVETRAN_DATABASE.dbt_dataflo_staging.stg_sf_contact
        
    ),leads as (
        select * from  FIVETRAN_DATABASE.dbt_dataflo_staging.stg_sf_lead
    ),opport as (
        select * from  FIVETRAN_DATABASE.dbt_dataflo_staging.stg_sf_opportunity    
),account_metric as (

    SELECT 
    users.USERNAME,
    acctname.account_name_count,
    acctype.account_type_count,
    contact.contact_name_count,
    leads.lead_name_count,
    leadstatus.lead_status_count,
    convert_lead.Converted_Leads,
    opport.Opportunities_this_quarter_count,
    opport_won.Opportunities_Won_Revenue,
    open_opport.Open_Opportunity_by_stage
    FROM users
    LEFT JOIN (select account_owner_id,COUNT(account_type) as account_type_count from account
     WHERE account_type is not null  
     group by account_owner_id) as acctype
    on users.user_id= acctype.account_owner_id

    LEFT JOIN (select account_owner_id,COUNT(account_name) as account_name_count from account
     WHERE account_name is not null  
     group by account_owner_id) as acctname
    on users.user_id= acctname.account_owner_id

    LEFT JOIN (select contact_owner_id, COUNT(contact_name) as contact_name_count from contact
     WHERE contact_name is not null  
     group by contact_owner_id) as contact
    on users.user_id= contact.contact_owner_id

    LEFT JOIN (select lead_owner_id, COUNT(full_name) as lead_name_count from leads
     WHERE full_name is not null  
     group by lead_owner_id) as leads
    on users.user_id= leads.lead_owner_id

    LEFT JOIN (select lead_owner_id, COUNT(status) as lead_status_count from leads
     WHERE status is not null  
     group by lead_owner_id) as leadstatus
    on users.user_id= leadstatus.lead_owner_id

    LEFT JOIN (select lead_owner_id, COUNT(is_converted) as Converted_Leads from leads
     WHERE is_converted is not null  
     group by lead_owner_id) as convert_lead
    on users.user_id= convert_lead.lead_owner_id

    LEFT JOIN (select opportunity_owner_id, COUNT(is_created_this_quarter) as Opportunities_this_quarter_count from opport
     WHERE company_name is not null  
     group by opportunity_owner_id) as opport
    on users.user_id= opport.opportunity_owner_id
    
    LEFT JOIN (select opportunity_owner_id, sum(amount) as Opportunities_Won_Revenue from opport
     WHERE stage_name is not null  and stage_name='Closed Won'
     group by opportunity_owner_id) as opport_won
    on users.user_id= opport_won.opportunity_owner_id

    LEFT JOIN (select opportunity_owner_id, COUNT(stage_name) as Open_Opportunity_by_stage from opport
     WHERE stage_name is not null  and stage_name <> 'Closed Won'
     group by opportunity_owner_id) as open_opport
    on users.user_id= open_opport.opportunity_owner_id



    where users.USERNAME is not null 
    
)
select * from account_metric
  );
