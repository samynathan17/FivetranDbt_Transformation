
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
    ),cases as (
        select * from  FIVETRAN_DATABASE.dbt_dataflo_staging.stg_sf_case 
   
),account_metric as (

    SELECT 
    opport.opportunity_owner_id,
    --opport.opportunity_account_id,
    --users.USERNAME,
    acctname.total_Accounts,
    accbyrevenue.Accounts_revenue,


    acctype.Accounts_by_Type,
    contact.total_Contacts,
    leads.total_Leads,
    case when quarterleads.thisquarter_total_Leads is not null then quarterleads.thisquarter_total_Leads ELSE 0 end as thisquarter_total_Leads,
    case when lastweekleads.lastweek_Leads_bylocation_count is not null then lastweekleads.lastweek_Leads_bylocation_count ELSE 0 end as lastweek_Leads_bylocation_count,
    case when thismonthleadstatus.thismonth_Leadsbystatus_count is not null then thismonthleadstatus.thismonth_Leadsbystatus_count ELSE 0 end as thismonth_Leadsbystatus_count,  
    oppor_total.total_Opportunities_count,
    case when thisquarter_opports.Opportunities_thisquarter_count is not null then thisquarter_opports.Opportunities_thisquarter_count ELSE 0 end as Opportunities_thisquarter_count ,
    opportbytype.thisyear_Opportunities_by_type,
     --leadstatus.Leads_by_status,
    
    -- Standard Metrics---

    opport_won.Opportunities_Won_Revenue,
    case when convert_lead.Converted_Leads is not null then convert_lead.Converted_Leads ELSE 0 end as Converted_Leads ,
    case when new_lead.NewLeads_by_name is not null then new_lead.NewLeads_by_name ELSE 0 end as NewLeads_by_name , 
    open_opport.Open_Opportunity_by_stage,
    TopSalesRep_won.TopSalesRep,
    lead_industry.NewLeads_by_industry,
    case when lost_opport.Opportunities_Lost is not null then lost_opport.Opportunities_Lost ELSE 0 end as Opportunities_Lost,
    case when lost_amount_opport.OpportunitiesLost_Amount is not null then lost_amount_opport.OpportunitiesLost_Amount ELSE 0 end as OpportunitiesLost_Amount,
    case when lostamountby_OppName.OpportunitiesLost_Amount_by_OppName is not null then lostamountby_OppName.OpportunitiesLost_Amount_by_OppName ELSE 0 end as OpportunitiesLost_Amount_by_OppName,
    --closedcase.Closed_Cases,
    expect_revenue.oppoExpectedRevenue,
    case when lead_source.NewLeads_by_source is not null then lead_source.NewLeads_by_source ELSE 0 end as NewLeads_by_source,
    won_opport.totalOpportunities_Won,
    wonamount_by_OppName.WonAmount_by_OppName,
    open_opporties.Open_Opportunities,
    open_opp_amount.Open_Opportunities_amount,
    open_opp_oppname.Open_Opportunities_oppname,

    opp_amount_agg.max_amount,
    opp_amount_agg.min_amount, 
    opp_amount_agg.total_amount


    FROM users
    LEFT JOIN (select account_owner_id,COUNT(account_name) as total_Accounts from account
     WHERE account_name is not null  
     group by account_owner_id) as acctname
    on users.user_id= acctname.account_owner_id

    LEFT JOIN (select account_owner_id,sum(ANNUAL_REVENUE) as Accounts_revenue from account
     WHERE account_name is not null and ANNUAL_REVENUE is not null 
     group by account_owner_id) as accbyrevenue
    on users.user_id= accbyrevenue.account_owner_id
    
    LEFT JOIN (select account_owner_id,COUNT(account_type) as Accounts_by_Type from account
     WHERE account_type is not null  
     group by account_owner_id) as acctype
    on users.user_id= acctype.account_owner_id

    LEFT JOIN (select contact_owner_id, COUNT(contact_name) as total_Contacts from contact
     WHERE contact_name is not null  
     group by contact_owner_id) as contact
    on users.user_id= contact.contact_owner_id

    LEFT JOIN (select lead_owner_id, COUNT(full_name) as total_Leads from leads
     WHERE full_name is not null 
     group by lead_owner_id) as leads
    on users.user_id= leads.lead_owner_id

    LEFT JOIN (select lead_owner_id, COUNT(full_name) as thisquarter_total_Leads from leads
     WHERE full_name is not null  and created_at between  date_trunc('quarter',thisquarter) and last_day(thisquarter,'quarter')
     group by lead_owner_id) as quarterleads
    on users.user_id= quarterleads.lead_owner_id

    LEFT JOIN (select lead_owner_id, count(COALESCE(street,city,state,postal_code,COUNTRY)) as lastweek_Leads_bylocation_count from leads
     WHERE country is not null  and created_at between  date_trunc('week',lastweek) and last_day(lastweek,'week')
     group by lead_owner_id) as lastweekleads
    on users.user_id= leads.lead_owner_id

    LEFT JOIN (select leads.lead_owner_id, COUNT(leads.status) as thismonth_Leadsbystatus_count from leads
     WHERE leads.status is not null  and  created_at between  date_trunc('month',thismonth) and last_day(thismonth,'month')
     group by lead_owner_id) as thismonthleadstatus
    on users.user_id= thismonthleadstatus.lead_owner_id

    LEFT JOIN (select opportunity_owner_id, count(company_name) as Opportunities_thisquarter_count from opport
     WHERE company_name is not null and created_at  between  date_trunc('quarter',thisquarter) and last_day(thisquarter,'quarter')
     group by opportunity_owner_id) as thisquarter_opports
    on users.user_id= thisquarter_opports.opportunity_owner_id

    -- LEFT JOIN (select leads.lead_owner_id, COUNT(leads.status) as Leads_by_status from leads
    --  WHERE leads.status is not null 
    --  group by lead_owner_id) as leadstatus
    -- on users.user_id= leadstatus.lead_owner_id

    LEFT JOIN (select opportunity_owner_id from opport) as opport --total oppor table 
    on users.user_id= opport.opportunity_owner_id

    LEFT JOIN (select opportunity_owner_id, COUNT(opportunity_type) as thisyear_Opportunities_by_type from opport
     WHERE opportunity_type is not null  and created_at  between  date_trunc('year',thisyear) and last_day(thisyear,'year')
     group by opportunity_owner_id) as opportbytype
    on users.user_id= opportbytype.opportunity_owner_id

    LEFT JOIN (select opportunity_owner_id, COUNT(company_name) as total_Opportunities_count from opport
     WHERE company_name is not null 
     group by opportunity_owner_id) as oppor_total
    on users.user_id= oppor_total.opportunity_owner_id

    --- Standard Metrics-----

    LEFT JOIN (select opportunity_owner_id, sum(amount) as Opportunities_Won_Revenue from opport
     WHERE stage_name is not null  and stage_name='Closed Won'
     group by opportunity_owner_id) as opport_won
    on users.user_id= opport_won.opportunity_owner_id
    --Key Metrics-pending

    LEFT JOIN (select lead_owner_id, COUNT(status) as Converted_Leads from leads
     WHERE status is not null  and is_converted='true'
     group by lead_owner_id) as convert_lead
    on users.user_id= convert_lead.lead_owner_id
    
    LEFT JOIN (select lead_owner_id, COUNT(full_name) as NewLeads_by_name from leads
     WHERE full_name is not null  and created_at between  date_trunc('week',lastweek) and last_day(lastweek,'week')
     group by lead_owner_id) as new_lead
    on users.user_id= new_lead.lead_owner_id
    
    LEFT JOIN (select opportunity_owner_id, COUNT(stage_name) as Open_Opportunity_by_stage from opport
     WHERE stage_name is not null  and stage_name <> 'Closed Won'
     group by opportunity_owner_id) as open_opport
    on users.user_id= open_opport.opportunity_owner_id

    --TopSalesRep- pending
    LEFT JOIN (select opportunity_owner_id, count(opportunity_owner_id) as TopSalesRep from opport
     WHERE opportunity_owner_id is not null  and stage_name='Closed Won'
     group by opportunity_owner_id) as TopSalesRep_won
    on users.user_id= TopSalesRep_won.opportunity_owner_id

    LEFT JOIN (select lead_owner_id, COUNT(industry) as NewLeads_by_industry from leads
     WHERE industry is not null and  is_converted <> 'true'
     group by lead_owner_id) as lead_industry
    on users.user_id= lead_industry.lead_owner_id

   -- Return On Investment Amount by Campaign --pending
   -- Return On Investment by Campaign Type --pending

    LEFT JOIN (select opportunity_owner_id, count(*) as Opportunities_Lost from opport
     WHERE stage_name is not null  and stage_name = 'Closed lost'
     group by opportunity_owner_id) as lost_opport
    on users.user_id= lost_opport.opportunity_owner_id
    
    LEFT JOIN (select opportunity_owner_id, sum(amount) as OpportunitiesLost_Amount from opport
     WHERE stage_name is not null  and stage_name = 'Closed lost'
     group by opportunity_owner_id) as lost_amount_opport
    on users.user_id= lost_amount_opport.opportunity_owner_id
    
    LEFT JOIN (select opportunity_owner_id, sum(amount) as OpportunitiesLost_Amount_by_OppName from opport
     WHERE company_name is not null  and stage_name = 'Closed lost'
     group by 1) as lostamountby_OppName
    on users.user_id= lostamountby_OppName.opportunity_owner_id

    -- inner JOIN (select id, count(*) as Closed_Cases from cases
    --  WHERE id is not null and status = 'Closed' group by 1) as closedcase
    -- on opport.OPPORTUNITY_ACCOUNT_ID=closedcase.account_id


    
    LEFT JOIN (select opportunity_owner_id, sum(EXPECTED_REVENUE) as oppoExpectedRevenue from opport
     WHERE CLOSE_DATE is not null  and stage_name <> 'Closed lost' and stage_name <> 'Closed Won'
     group by 1) as expect_revenue
    on users.user_id= expect_revenue.opportunity_owner_id


    LEFT JOIN (select lead_owner_id, COUNT(LEAD_SOURCE) as NewLeads_by_source from leads
     WHERE LEAD_SOURCE is not null  and created_at between  date_trunc('week',lastweek) and last_day(lastweek,'week')
     group by lead_owner_id) as lead_source
    on users.user_id= lead_source.lead_owner_id


   
    LEFT JOIN (select opportunity_owner_id, count(stage_name) as totalOpportunities_Won from opport
     WHERE stage_name is not null  and stage_name = 'Closed Won'
     group by opportunity_owner_id) as won_opport
    on users.user_id= won_opport.opportunity_owner_id

    LEFT JOIN (select opportunity_owner_id,sum(amount) as WonAmount_by_OppName from opport
     WHERE stage_name is not null  and stage_name = 'Closed Won'
     group by 1) as wonamount_by_OppName
    on users.user_id= wonamount_by_OppName.opportunity_owner_id

    LEFT JOIN (select opportunity_owner_id, COUNT(stage_name) as Open_Opportunities from opport
     WHERE stage_name is not null  and stage_name <> 'Closed Won' and stage_name <> 'Closed lost'
     group by opportunity_owner_id) as open_opporties
    on users.user_id= open_opporties.opportunity_owner_id

    LEFT JOIN (select opportunity_owner_id, sum(amount) as Open_Opportunities_amount from opport
     WHERE stage_name is not null  and stage_name <> 'Closed Won' and stage_name <> 'Closed lost'
     group by opportunity_owner_id) as open_opp_amount
    on users.user_id= open_opp_amount.opportunity_owner_id

    LEFT JOIN (select opportunity_owner_id, sum(amount) as Open_Opportunities_oppname from opport
     WHERE company_name is not null  and stage_name <> 'Closed Won' and stage_name <> 'Closed lost'
     group by opportunity_owner_id) as open_opp_oppname
    on users.user_id= open_opp_oppname.opportunity_owner_id

    LEFT JOIN (select opportunity_owner_id, MAX(Amount) as max_amount, MIN(Amount) as min_amount, SUM(Amount) as total_amount from opport
     WHERE opportunity_owner_id is not null 
     group by 1) as opp_amount_agg
    on users.user_id= opp_amount_agg.opportunity_owner_id

    where users.USERNAME is not null and user_id is not null
    
)
select distinct *  from account_metric  where OPPORTUNITY_OWNER_ID is not null
  );
