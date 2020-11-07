--Dimension table account & user join
with account as (

    SELECT * FROM {{ ref('stg_sf_account') }}
),account_user as(
    SELECT * FROM {{ ref('stg_sf_user') }}
)

SELECT 
    account_user.*,
    account_id,
    account_name,
    -- keys
    parent_id,
    account_owner_id,
    -- logistics
    account_type, 
    account_number
    
    
    FROM account
    LEFT JOIN 
        (select 
            user_id,
            username,
            user_type,
            employee_number,
            company_name as user_companyname,
            TIME_ZONE_SID_KEY
       from account_user ) as account_user
        on account.account_owner_id = account_user.user_id

