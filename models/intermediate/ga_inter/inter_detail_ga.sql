with aq_overview as (

    SELECT * FROM {{ ref('stg_ga_aq_overview') }}
),aud_geoloc as(
    SELECT * FROM {{ ref('stg_ga_aud_geoloc') }}
),audi_overview as(
    SELECT * FROM {{ ref('stg_ga_audi_overview') }}
),audi_techno as(
    SELECT * FROM {{ ref('stg_ga_audi_techno') }}
),ga as(
    SELECT 
    aq_overview.*,
    aud_geoloc.*,
    audi_overview.*,
    audi_techno.*
    
 from aq_overview 
left join aud_geoloc on aq_overview.ACQU_ACCOUNT_ID=aud_geoloc.AUD_GEOLOC_ACCOUNT_ID
left join audi_overview on aq_overview.ACQU_ACCOUNT_ID=audi_overview.AUD_OVERVIEW_ACCOUNT_ID
left join audi_techno on aq_overview.ACQU_ACCOUNT_ID=audi_techno.AUD_TECH_ACCOUNT_ID
)

SELECT count(AUD_OVERVIEW_ACCOUNT_ID) from audi_overview where cast(GETDATE() as date) between '30/10/2020' and '5/11/2020'

--30 Oct 2020-5 Nov 2020	

