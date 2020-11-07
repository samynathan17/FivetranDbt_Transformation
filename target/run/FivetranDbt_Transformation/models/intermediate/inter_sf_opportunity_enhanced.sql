
  create or replace  view FIVETRAN_DATABASE.dbt_dataflo_staging.inter_sf_opportunity_enhanced  as (
    --Fact table for DataFlo metrics

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
    opport.opportunity_owner_id,
    --users.USERNAME,
    acctname.Accounts,
    acctype.Accounts_by_Type,
    contact.Contacts,
    leads.Leads,
    leadstatus.Leads_by_status,
    lastweekleads.Leadsbylocationlast7days,
    opports.Opportunities_this_quarter,
    opport_by_type.Opportunities_by_type,
    opport_won.Opportunities_Won_Revenue,
    convert_lead.Converted_Leads,
    new_lead.New_Leads, 
    open_opport.Open_Opportunity_by_stage,
    lead_industry.NewLeads_by_industry,
    lost_opport.Opportunities_Lost,
    lost_amount_opport.OpportunitiesLost_Amount_by_Owner,
    lost_amount_opport_by_OppName.OpportunitiesLost_Amount_by_Opp_Name,
    won_opport.Opportunities_Won,
    won_amount_opport_by_OppName.OpportunitiesWon_Amount_by_Opp_Name,

    opp_amount_agg.max_amount,
    opp_amount_agg.min_amount, 
    opp_amount_agg.total_amount


    FROM users
    LEFT JOIN (select account_owner_id,COUNT(account_name) as Accounts from account
     WHERE account_name is not null  
     group by account_owner_id) as acctname
    on users.user_id= acctname.account_owner_id

    LEFT JOIN (select account_owner_id,COUNT(account_type) as Accounts_by_Type from account
     WHERE account_type is not null  
     group by account_owner_id) as acctype
    on users.user_id= acctype.account_owner_id

    LEFT JOIN (select contact_owner_id, COUNT(contact_name) as Contacts from contact
     WHERE contact_name is not null  
     group by contact_owner_id) as contact
    on users.user_id= contact.contact_owner_id

    LEFT JOIN (select lead_owner_id, COUNT(full_name) as Leads from leads
     WHERE full_name is not null  
     group by lead_owner_id) as leads
    on users.user_id= leads.lead_owner_id
   --Leads by location -pending
    LEFT JOIN (select lead_owner_id, COUNT(country) as Leadsbylocationlast7days from leads
     WHERE full_name is not null  and DATEADD(DD,-7,GETDATE())
     group by lead_owner_id) as lastweekleads
    on users.user_id= leads.lead_owner_id

    LEFT JOIN (select lead_owner_id, COUNT(status) as Leads_by_status from leads
     WHERE status is not null  
     group by lead_owner_id) as leadstatus
    on users.user_id= leadstatus.lead_owner_id

    LEFT JOIN (select opportunity_owner_id from opport
     WHERE company_name is not null) as opport
    on users.user_id= opport.opportunity_owner_id

    LEFT JOIN (select opportunity_owner_id, 
    COALESCE(sum(CASE WHEN is_created_this_quarter THEN 1 ELSE 0 END),0) as Opportunities_this_quarter from opport
     WHERE company_name is not null 
     group by opportunity_owner_id) as opports
    on users.user_id= opports.opportunity_owner_id

    LEFT JOIN (select opportunity_owner_id, COUNT(opportunity_type) as Opportunities_by_type from opport
     WHERE opportunity_type is not null  
     group by opportunity_owner_id) as opport_by_type
    on users.user_id= opport_by_type.opportunity_owner_id

    LEFT JOIN (select opportunity_owner_id, sum(amount) as Opportunities_Won_Revenue from opport
     WHERE stage_name is not null  and stage_name='Closed Won'
     group by opportunity_owner_id) as opport_won
    on users.user_id= opport_won.opportunity_owner_id
    --Key Metrics-pending

    LEFT JOIN (select lead_owner_id, COUNT(status) as Converted_Leads from leads
     WHERE status is not null  and is_converted='true'
     group by lead_owner_id) as convert_lead
    on users.user_id= convert_lead.lead_owner_id
    
    LEFT JOIN (select lead_owner_id, COUNT(full_name) as New_Leads from leads
     WHERE full_name is not null  
     group by lead_owner_id) as new_lead
    on users.user_id= new_lead.lead_owner_id
    
    LEFT JOIN (select opportunity_owner_id, COUNT(stage_name) as Open_Opportunity_by_stage from opport
     WHERE stage_name is not null  and stage_name <> 'Closed Won'
     group by opportunity_owner_id) as open_opport
    on users.user_id= open_opport.opportunity_owner_id
    --Top Sales Rep- pending
    LEFT JOIN (select lead_owner_id, COUNT(industry) as NewLeads_by_industry from leads
     WHERE industry is not null  
     group by lead_owner_id) as lead_industry
    on users.user_id= lead_industry.lead_owner_id
   -- Return On Investment Amount by Campaign --pending
   -- Return On Investment by Campaign Type --pending

    LEFT JOIN (select opportunity_owner_id, count(stage_name) as Opportunities_Lost from opport
     WHERE stage_name is not null  and stage_name = 'Closed lost'
     group by opportunity_owner_id) as lost_opport
    on users.user_id= lost_opport.opportunity_owner_id
    
    LEFT JOIN (select opportunity_owner_id, sum(amount) as OpportunitiesLost_Amount_by_Owner from opport
     WHERE stage_name is not null  and stage_name = 'Closed lost'
     group by opportunity_owner_id) as lost_amount_opport
    on users.user_id= lost_amount_opport.opportunity_owner_id
    
    LEFT JOIN (select opportunity_owner_id, sum(amount) as OpportunitiesLost_Amount_by_Opp_Name from opport
     WHERE stage_name is not null  and stage_name = 'Closed lost'
     group by 1) as lost_amount_opport_by_OppName
    on users.user_id= lost_amount_opport.opportunity_owner_id
   
    LEFT JOIN (select opportunity_owner_id, count(stage_name) as Opportunities_Won from opport
     WHERE stage_name is not null  and stage_name = 'Closed Won'
     group by opportunity_owner_id) as won_opport
    on users.user_id= won_opport.opportunity_owner_id

    LEFT JOIN (select opportunity_owner_id,sum(amount) as OpportunitiesWon_Amount_by_Opp_Name from opport
     WHERE stage_name is not null  and stage_name = 'Closed Won'
     group by 1) as won_amount_opport_by_OppName
    on users.user_id= won_amount_opport_by_OppName.opportunity_owner_id

    LEFT JOIN (select opportunity_owner_id, MAX(Amount) as max_amount, MIN(Amount) as min_amount, SUM(Amount) as total_amount from opport
     WHERE opportunity_owner_id is not null 
     group by 1) as opp_amount_agg
    on users.user_id= opp_amount_agg.opportunity_owner_id

    where users.USERNAME is not null and user_id is not null
    
)
select distinct *  from account_metric  where OPPORTUNITY_OWNER_ID is not null
  );
