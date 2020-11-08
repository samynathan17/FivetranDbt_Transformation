


    WITH source AS (

    select * from salesforce.user

    ),renamed as (
        select 
          id as user_id,
          username,
          name,
          city,
          state,
          manager_id,
          user_role_id,
          company_name,
          email,
          user_type,
          employee_number,
          TIME_ZONE_SID_KEY,
          CREATED_DATE,
          CREATED_BY_ID,
          LAST_MODIFIED_DATE,
          LAST_MODIFIED_BY_ID,
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