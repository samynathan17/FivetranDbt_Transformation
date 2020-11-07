WITH AUDIENCE_OVERVIEW AS (

      
     select * from DATAFLO_GOOGLEANALYTICS.AUDIENCE_OVERVIEW  

)

select 
    ACCOUNT_ID	 AS AUD_OVERVIEW_ACCOUNT_ID ,
    END_DATE	 AS AUD_OVERVIEW_END_DATE ,
    GA_AVGSESSIONDURATION	 AS AUD_OVERVIEW_GA_AVGSESSIONDURATION ,
    GA_BOUNCERATE	 AS AUD_OVERVIEW_GA_BOUNCERATE ,
    GA_DATE	 AS AUD_OVERVIEW_GA_DATE ,
    GA_NEWUSERS	 AS AUD_OVERVIEW_GA_NEWUSERS ,
    GA_PAGEVIEWS	 AS AUD_OVERVIEW_GA_PAGEVIEWS ,
    GA_PAGEVIEWSPERSESSION	 AS AUD_OVERVIEW_GA_PAGEVIEWSPERSESSION ,
    GA_SESSIONS	 AS AUD_OVERVIEW_GA_SESSIONS ,
    GA_SESSIONSPERUSER	 AS AUD_OVERVIEW_GA_SESSIONSPERUSER ,
    GA_USERS	 AS AUD_OVERVIEW_GA_USERS ,
    PROFILE_ID	 AS AUD_OVERVIEW_PROFILE_ID ,
    START_DATE	 AS AUD_OVERVIEW_START_DATE ,
    WEB_PROPERTY_ID AS AUD_OVERVIEW_WEB_PROPERTY_ID 
    
 from AUDIENCE_OVERVIEW