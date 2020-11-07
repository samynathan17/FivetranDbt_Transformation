with aq_overview as (

    SELECT * FROM FIVETRAN_DATABASE.dbt_dataflo_staging.stg_ga_aq_overview
),aud_geoloc as(
    SELECT * FROM FIVETRAN_DATABASE.dbt_dataflo_staging.stg_ga_aud_geoloc
),audi_overview as(
    SELECT * FROM FIVETRAN_DATABASE.dbt_dataflo_staging.stg_ga_audi_overview
),audi_techno as(
    SELECT * FROM FIVETRAN_DATABASE.dbt_dataflo_staging.stg_ga_audi_techno
)

SELECT 
    aq_overview.*,
    aud_geoloc.*,
    audi_overview.*,
    audi_techno.*
    
 from aq_overview 
left join aud_geoloc on aq_overview.ACCOUNT_ID=aud_geoloc.ACCOUNT_ID
left join audi_overview on aq_overview.ACCOUNT_ID=audi_overview.ACCOUNT_ID
left join audi_techno on aq_overview.ACCOUNT_ID=audi_techno.ACCOUNT_ID