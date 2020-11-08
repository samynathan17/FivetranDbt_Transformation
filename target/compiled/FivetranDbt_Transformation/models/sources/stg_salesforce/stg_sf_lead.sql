



 with source as (

  select * from salesforce.lead

  ),renamed as (
    select

        id as lead_id,
        -- keys
        owner_id as lead_owner_id,
        converted_opportunity_id as opportunity_id,
        converted_account_id as account_id,
        converted_contact_id as contact_id,

        -- conversion pipeline
        lead_source,
        status,
        is_converted,
        converted_date,
        industry,
        annual_revenue,

        -- contact
        first_name,
        last_name,
        name as full_name,
        title,
        email,
        phone as work_phone,
        mobile_phone,
        street,
        city,
        state,
        postal_code,
        COUNTRY,
       
        -- outreach
        is_unread_by_owner,

        -- company context
        company as company_name,
        city as company_city,
        website,
        number_of_employees,

        -- metadata
        last_activity_date,
        created_date as created_at,
        last_modified_date as updated_at,
        IS_DELETED,
        clean_status,
        individual_id,
        product_interest_c as product_interest,
        current_generators_c as current_generators,
        DATEADD(DD,-7,GETDATE()) AS lastweek,
        DATEADD(DD,0,GETDATE()) AS thisweek,
        DATEADD(MM,-1,GETDATE()) AS lastmonth,
        DATEADD(MM,0,GETDATE()) AS thismonth,
        extract(quarter from date_trunc('quarter', GETDATE())::date - 1) AS lastquarter,
        CURRENT_DATE() AS thisquarter,
        DATEADD(year,-1,GETDATE()) AS lastyear,
        DATEADD(year,0,GETDATE()) AS thisyear

    from source  
    )
    


select * from renamed