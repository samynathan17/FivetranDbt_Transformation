


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
        IS_DELETED
     from source
    )


select * from renamed