


    WITH source AS (

    select * from salesforce.user_role

    ), renamed as (

    select 

      id as user_role_id,
      name as role_name,
      developer_name,
      _fivetran_deleted as IS_DELETED

    from source
    )


select * from renamed