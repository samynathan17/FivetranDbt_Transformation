


    WITH source AS (

    select * from salesforce.contact 

    ),renamed as (
      select
        id as contact_id,
        name as contact_name,
       --keys
        account_id,
        owner_id as contact_owner_id,

        title,
        email as contact_email,
        department,
        assistant_name,
        lead_source,
        clean_status,
        level_c,
        languages_c ,
        CREATED_DATE,
        IS_DELETED,
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