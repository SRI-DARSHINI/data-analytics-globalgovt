USE DATABASE GLOBALGOVT;
use schema staging;

--market size (american trend)/consumer composition//
create or replace table americancommunitysurvey as
SELECT
    ACSA.CENSUS_SUBJECT,
    ACSA.FREQUENCY,
    ACSA.MEASURE,
    ACSA.MEASUREMENT_PERIOD,
    ACSA.MEASUREMENT_TYPE,
    ACSA.RACE,
    ACSA.SERIES_ID,
    ACSA.SERIES_LEVEL_1,
    ACSA.SERIES_LEVEL_2,
    ACSA.SERIES_LEVEL_3,
    ACSA.SERIES_LEVEL_4,
    ACSA.SERIES_LEVEL_5,
    ACSA.SERIES_LEVEL_6,
    ACSA.SERIES_TYPE,
    ACSA.UNIT AS ATTRIBUTE_UNIT,
    ACSA.VARIABLE AS ATTRIBUTE_VARIABLE,
    ACSA.VARIABLE_NAME AS ATTRIBUTE_VARIABLE_NAME,
    ACTS.DATE,
    ACTS.GEO_ID,
    ACTS.VALUE,
    ACTS.UNIT AS TIMESERIES_UNIT,
    ACTS.VARIABLE AS TIMESERIES_VARIABLE,
    ACTS.VARIABLE_NAME AS TIMESERIES_VARIABLE_NAME
FROM
    GLOBAL_GOVERNMENT.CYBERSYN.AMERICAN_COMMUNITY_SURVEY_ATTRIBUTES ACSA
JOIN
    GLOBAL_GOVERNMENT.CYBERSYN.AMERICAN_COMMUNITY_SURVEY_TIMESERIES ACTS
ON
    ACSA.VARIABLE = ACTS.VARIABLE limit 1000;


--location plan poi
create or replace table geographyindexrelationships as
SELECT
    GI.GEO_ID,
    GI.GEO_NAME,
    GI.ISO_3166_2_CODE,
    GI.ISO_ALPHA2,
    GI.ISO_ALPHA3,
    GI.ISO_NAME,
    GI.ISO_NUMERIC_CODE,
    GR.GEO_ID AS RELATED_GEO_ID,
    GR.GEO_NAME AS RELATED_GEO_NAME,
    GR.LEVEL AS RELATED_LEVEL,
    GR.RELATIONSHIP_TYPE
FROM
    GLOBAL_GOVERNMENT.CYBERSYN.GEOGRAPHY_INDEX GI
JOIN
    GLOBAL_GOVERNMENT.CYBERSYN.GEOGRAPHY_RELATIONSHIPS GR
ON
    GI.GEO_ID = GR.GEO_ID;


--industrial output(industry asset and production)/consumer credit behavior
create or replace table globalgovt.staging.federalreserve as
SELECT 
    a.CATEGORY,
    a.FREQUENCY,
    a.MEASURE,
    a.NAICS,
    a.SEASONALLY_ADJUSTED,
    a.SECTOR,
    a.TERMS,
    a.UNIT AS ATTRIBUTE_UNIT,
    a.VARIABLE,
    a.VARIABLE_NAME AS ATTRIBUTE_VARIABLE_NAME,
    t.DATE,
    t.GEO_ID,
    t.VALUE,
    t.UNIT AS TIMESERIES_UNIT,
    t.VARIABLE_NAME AS TIMESERIES_VARIABLE_NAME
FROM 
   GLOBAL_GOVERNMENT.CYBERSYN.FEDERAL_RESERVE_ATTRIBUTES a
JOIN 
   GLOBAL_GOVERNMENT.CYBERSYN.FEDERAL_RESERVE_TIMESERIES t
    ON a.VARIABLE = t.VARIABLE;

--macroeconomic trends
create or replace table ustreasury as
SELECT 
    ts.DATE,
    ts.GEO_ID,
    ts.UNIT AS TIMESERIES_UNIT,
    ts.VALUE,
    ts.VARIABLE,
    ts.VARIABLE_NAME AS TIMESERIES_VARIABLE_NAME,
    attr.FREQUENCY,
    attr.MEASURE,
    attr.MEASUREMENT_TYPE,
    attr.TAX_CATEGORY,
    attr.UNIT AS ATTRIBUTES_UNIT,
    attr.VARIABLE_NAME AS ATTRIBUTES_VARIABLE_NAME
FROM 
    GLOBAL_GOVERNMENT.CYBERSYN.US_TREASURY_TIMESERIES ts
JOIN 
    GLOBAL_GOVERNMENT.CYBERSYN.US_TREASURY_ATTRIBUTES attr
ON 
    ts.VARIABLE = attr.VARIABLE;


--global comparison
CREATE OR REPLACE TABLE world_bank as
SELECT 
    wbattr.ESG_PILLAR,
    wbattr.FREQUENCY,
    wbattr.MEASURE,
    wbattr.RELATED_TOPICS,
    wbattr.SOURCE,
    wbattr.SOURCE_NOTE,
    wbattr.UNIT AS ATTRIBUTE_UNIT,
    wbtime.DATE,
    wbtime.GEO_ID,
    wbtime.UNIT AS TIMESERIES_UNIT,
    wbtime.VALUE,
    wbtime.VARIABLE,
    wbtime.VARIABLE_NAME
FROM 
    GLOBAL_GOVERNMENT.CYBERSYN.WORLD_BANK_ATTRIBUTES AS wbattr
JOIN 
    GLOBAL_GOVERNMENT.CYBERSYN.WORLD_BANK_TIMESERIES AS wbtime
ON 
    wbattr.VARIABLE = wbtime.VARIABLE;

CREATE OR REPLACE TABLE world_bank AS
SELECT 
    wbattr.ESG_PILLAR,
    wbattr.FREQUENCY,
    wbattr.MEASURE,
    wbattr.RELATED_TOPICS,
    wbattr.SOURCE,
    wbattr.SOURCE_NOTE,
    wbattr.UNIT AS ATTRIBUTE_UNIT,
    wbtime.DATE,
    wbtime.GEO_ID,
    wbtime.UNIT AS TIMESERIES_UNIT,
    wbtime.VALUE,
    wbtime.VARIABLE,
    wbtime.VARIABLE_NAME
FROM 
    GLOBAL_GOVERNMENT.CYBERSYN.WORLD_BANK_ATTRIBUTES AS wbattr
JOIN 
    GLOBAL_GOVERNMENT.CYBERSYN.WORLD_BANK_TIMESERIES AS wbtime
ON 
    wbattr.VARIABLE = wbtime.VARIABLE
WHERE 
    wbattr.ESG_PILLAR IS NOT NULL AND TRIM(wbattr.ESG_PILLAR) != 'blank';

--international trade trends
create or replace table worldtradeorganization as
SELECT 
    t.DATE,
    t.PARTNER_GEO_ID,
    t.REPORTING_GEO_ID,
    t.UNIT AS TIMESERIES_UNIT,
    t.VALUE,
    t.VARIABLE,
    a.FREQUENCY,
    a.MEASURE,
    a.MEASUREMENT_TYPE,
    a.PRODUCT_SECTOR,
    a.UNIT AS ATTRIBUTE_UNIT,
    a.VARIABLE_NAME,
    a.WTO_INDICATOR
FROM GLOBAL_GOVERNMENT.CYBERSYN.WORLD_TRADE_ORGANIZATION_TIMESERIES t
JOIN GLOBAL_GOVERNMENT.CYBERSYN.WORLD_TRADE_ORGANIZATION_ATTRIBUTES a
    ON t.VARIABLE = a.VARIABLE;

--export market(demand)
create or replace table usdepartmentagriculturecommodities as
SELECT 
    a.COMMODITY_TYPE,
    a.FREQUENCY,
    a.MEASURE,
    a.UNIT AS ATTRIBUTE_UNIT,
    a.VARIABLE,
    a.VARIABLE_NAME,
    t.CALENDAR_YEAR,
    t.DATE,
    t.GEO_ID,
    t.MARKETING_YEAR,
    t.UNIT AS TIMESERIES_UNIT,
    t.VALUE
FROM 
    GLOBAL_GOVERNMENT.CYBERSYN.US_DEPARTMENT_OF_AGRICULTURE_COMMODITIES_ATTRIBUTES a
JOIN 
    GLOBAL_GOVERNMENT.CYBERSYN.US_DEPARTMENT_OF_AGRICULTURE_COMMODITIES_TIMESERIES t
ON 
    a.VARIABLE = t.VARIABLE;

--labor market dymanics(demographic employment)
CREATE TABLE GLOBALGOVT.STAGING.internationallabourorganization AS
SELECT 
    a.AGE_GROUP,
    a.ECONOMIC_ACTIVITY,
    a.ECONOMIC_CLASS,
    a.EMPLOYMENT_STATUS,
    a.FREQUENCY,
    a.GEOGRAPHY_TYPE,
    a.MEASURE,
    a.MEASUREMENT_TYPE,
    a.OCCUPATION,
    a.SERIES_ID,
    a.SEX,
    a.UNIT AS ATTRIBUTE_UNIT,
    a.VARIABLE,
    a.VARIABLE_NAME,
    t.DATE,
    t.GEO_ID,
    t.UNIT AS TIMESERIES_UNIT,
    t.VALUE,
    t.VALUE_COMMENT
FROM 
    GLOBAL_GOVERNMENT.CYBERSYN.INTERNATIONAL_LABOUR_ORGANIZATION_ATTRIBUTES a
JOIN 
    GLOBAL_GOVERNMENT.CYBERSYN.INTERNATIONAL_LABOUR_ORGANIZATION_TIMESERIES t
ON 
    a.VARIABLE = t.VARIABLE;

--unemployment trends
CREATE TABLE GLOBALGOVT.STAGING.uslaborunemployment AS
SELECT 
    uict.DATE,
    uict.GEO_ID,
    uict.UNIT AS TIMESERIES_UNIT,
    uict.VALUE,
    uict.VARIABLE,
    uict.VARIABLE_NAME AS TIMESERIES_VARIABLE_NAME,
    uica.FREQUENCY,
    uica.MEASURE,
    uica.MEASUREMENT_TYPE,
    uica.UNIT AS ATTRIBUTES_UNIT,
    uica.VARIABLE_NAME AS ATTRIBUTES_VARIABLE_NAME
FROM 
    GLOBAL_GOVERNMENT.CYBERSYN.US_DEPARTMENT_OF_LABOR_UNEMPLOYMENT_INSURANCE_CLAIMS_TIMESERIES uict
JOIN 
    GLOBAL_GOVERNMENT.CYBERSYN.US_DEPARTMENT_OF_LABOR_UNEMPLOYMENT_INSURANCE_CLAIMS_ATTRIBUTES uica
ON 
    uict.VARIABLE = uica.VARIABLE;


    
--crime
CREATE or replace TABLE GLOBALGOVT.STAGING.topcrime AS
SELECT GEO_ID, SUM(VALUE) AS total_crimes
FROM GLOBAL_GOVERNMENT.CYBERSYN.FBI_CRIME_TIMESERIES
GROUP BY GEO_ID
ORDER BY total_crimes DESC;

select * from GLOBALGOVT.STAGING.TOPCRIME limit 20;

-- Store output in a new table housing_urban_development in GLOBALGOVT.STAGING schema
CREATE TABLE GLOBALGOVT.STAGING.HOUSING_URBAN_DEVELOPMENT AS
SELECT 
    t.DATE,
    t.GEO_ID,
    t.UNIT AS TIME_SERIES_UNIT,
    t.VALUE,
    t.VARIABLE,
    t.VARIABLE_NAME AS TIME_SERIES_VARIABLE_NAME,
    a.FREQUENCY,
    a.MEASURE,
    a.UNIT AS ATTRIBUTE_UNIT,
    a.VARIABLE_NAME AS ATTRIBUTE_VARIABLE_NAME
FROM 
    GLOBAL_GOVERNMENT.CYBERSYN.HOUSING_URBAN_DEVELOPMENT_TIMESERIES t
JOIN 
    GLOBAL_GOVERNMENT.CYBERSYN.HOUSING_URBAN_DEVELOPMENT_ATTRIBUTES a
ON 
    t.VARIABLE = a.VARIABLE
WHERE 
    t.UNIT = a.UNIT -- Optional: Ensure unit compatibility
ORDER BY 
    t.DATE, t.GEO_ID;

    --economic impact
    --us federal reserve
-- Step 1: Extract time-series data for financial trends
SELECT 
    DATE,
    SECTOR,
    ATTRIBUTE_VARIABLE_NAME AS Variable_Name,
    VALUE AS Value,
    ATTRIBUTE_UNIT AS Unit,
    FREQUENCY,
    SEASONALLY_ADJUSTED
FROM 
    GLOBALGOVT.staging.federalreserve
WHERE 
    FREQUENCY = 'Monthly' -- Focus on monthly data for time-series analysis
    AND SEASONALLY_ADJUSTED = 'Seasonally adjusted';

-- Step 2: Extract sector-based insights
SELECT 
    SECTOR,
    NAICS,
    AVG(VALUE) AS Avg_Value,
    MIN(VALUE) AS Min_Value,
    MAX(VALUE) AS Max_Value,
    COUNT(*) AS Data_Points
FROM 
    GLOBALGOVT.staging.federalreserve
WHERE 
    CATEGORY IS NOT NULL -- Ensure valid category data
GROUP BY 
    SECTOR, NAICS;

    -- Step 3: Aggregate data for econometric modeling
WITH PolicyImpact AS (
    SELECT 
        DATE,
        ATTRIBUTE_VARIABLE_NAME AS Variable,
        VALUE,
        GEO_ID,
        CATEGORY
    FROM 
        GLOBALGOVT.staging.federalreserve
    WHERE 
        DATE BETWEEN '2024-01-01' AND '2024-12-31'
)
SELECT 
    Variable,
    GEO_ID,
    MONTH(DATE) AS Month,
    AVG(VALUE) AS Avg_Value,
    STDDEV(VALUE) AS Std_Dev
FROM 
    PolicyImpact
GROUP BY 
    Variable, GEO_ID, MONTH(DATE)
ORDER BY 
    Variable, GEO_ID, Month;

    --us department of transportation
    -- Query to analyze domestic air traffic patterns, airline market share, and performance
SELECT 
    ts.DATE,
    ai.AIRPORT_NAME AS ORIGIN_AIRPORT,
    ai2.AIRPORT_NAME AS DESTINATION_AIRPORT,
    ts.SERVICE_CLASS,
    ts.FLIGHT_TYPE_ID,
    attr.VARIABLE_NAME,
    attr.MEASURE,
    attr.UNIT,
    ts.AIRCRAFT_CARRIER_ID,
    COUNT(ts.AIRCRAFT_ID) AS TOTAL_FLIGHTS,
    SUM(attr.MEASURE) AS TOTAL_MEASURE
FROM 
    staging.US_DEPARTMENT_OF_TRANSPORTATION_TIMESERIES ts
JOIN 
    staging.AIRPORT_INDEX ai
    ON ts.ORIGIN_AIRPORT_ID = ai.AIRPORT_ID
JOIN 
    staging.AIRPORT_INDEX ai2
    ON ts.DESTINATION_AIRPORT_ID = ai2.AIRPORT_ID
JOIN 
    staging.US_DEPARTMENT_OF_TRANSPORTATION_ATTRIBUTES attr
    ON ts.FLIGHT_TYPE_ID = attr.VARIABLE
WHERE 
    attr.VARIABLE_NAME IN ('Passenger Count', 'Cargo Weight')
    AND ts.DATE BETWEEN '2023-01-01' AND '2023-12-31'
    AND ai.STATE_GEO_ID = 'US'
GROUP BY 
    ts.DATE, ai.AIRPORT_NAME, ai2.AIRPORT_NAME, ts.SERVICE_CLASS, ts.FLIGHT_TYPE_ID, 
    attr.VARIABLE_NAME, attr.MEASURE, attr.UNIT, ts.AIRCRAFT_CARRIER_ID
ORDER BY 
    ts.DATE, TOTAL_FLIGHTS DESC;

    --ustreasury finance health
    create or replace table ustreasury as
    SELECT 
    ts.DATE,
    ts.GEO_ID,
    ts.UNIT AS TIMESERIES_UNIT,
    ts.VALUE,
    ts.VARIABLE,
    ts.VARIABLE_NAME,
    attr.FREQUENCY,
    attr.MEASURE,
    attr.MEASUREMENT_TYPE,
    attr.TAX_CATEGORY,
    attr.UNIT AS ATTRIBUTE_UNIT
FROM 
    GLOBAL_GOVERNMENT.cybersyn.US_TREASURY_TIMESERIES ts
JOIN 
    GLOBAL_GOVERNMENT.cybersyn.US_TREASURY_ATTRIBUTES attr
ON 
    ts.VARIABLE = attr.VARIABLE
WHERE 
    ts.VARIABLE_NAME = attr.VARIABLE_NAME;

SELECT 
    DATE,
    GEO_ID,
    VALUE,
    VARIABLE,
    VARIABLE_NAME,
    TIMESERIES_UNIT,
    FREQUENCY,
    MEASURE,
    MEASUREMENT_TYPE,
    TAX_CATEGORY,
    ATTRIBUTE_UNIT
FROM 
    GLOBALGOVT.staging.USTREASURY
WHERE 
    FREQUENCY = 'Daily'
ORDER BY 
    DATE, VARIABLE_NAME;

    
-- Query to Extract ESG Data for Analysis
SELECT
    ESG_PILLAR,
    FREQUENCY,
    MEASURE,
    GEO_ID,
    VARIABLE_NAME,
    ATTRIBUTE_UNIT,
    DATE,
    VALUE,
    SOURCE,
    SOURCE_NOTE
FROM
    WORLD_BANK

ORDER BY
    ESG_PILLAR,
    GEO_ID,
    DATE;

--Use INTERNATIONAL_URBAN_LABOUR_ORGANIZATION_TIMESERIES for labor trends
create or replace table internationallabourorganization as
SELECT 
    attributes.AGE_GROUP,
    attributes.ECONOMIC_ACTIVITY,
    attributes.ECONOMIC_CLASS,
    attributes.EMPLOYMENT_STATUS,
    attributes.FREQUENCY,
    attributes.GEOGRAPHY_TYPE,
    attributes.MEASURE,
    attributes.MEASUREMENT_TYPE,
    attributes.OCCUPATION,
    attributes.SERIES_ID,
    attributes.SEX,
    attributes.UNIT AS ATTRIBUTE_UNIT,
    attributes.VARIABLE,
    attributes.VARIABLE_NAME,
    timeseries.DATE,
    timeseries.GEO_ID,
    timeseries.UNIT AS TIMESERIES_UNIT,
    timeseries.VALUE,
    timeseries.VALUE_COMMENT
FROM 
    GLOBAL_GOVERNMENT.cybersyn.INTERNATIONAL_LABOUR_ORGANIZATION_ATTRIBUTES attributes
JOIN 
    GLOBAL_GOVERNMENT.cybersyn.INTERNATIONAL_LABOUR_ORGANIZATION_TIMESERIES timeseries
ON 
    attributes.VARIABLE = timeseries.VARIABLE;
    
WITH AggregatedData AS (
    SELECT 
        attributes.AGE_GROUP,
        attributes.ECONOMIC_ACTIVITY,
        attributes.ECONOMIC_CLASS,
        attributes.EMPLOYMENT_STATUS,
        attributes.GEOGRAPHY_TYPE,
        attributes.OCCUPATION,
        attributes.SEX,
        attributes.MEASURE,
        attributes.VARIABLE_NAME,
        attributes.DATE,
        AVG(attributes.VALUE) AS Average_Value,
        MAX(attributes.VALUE) AS Max_Value,
        MIN(attributes.VALUE) AS Min_Value
    FROM 
        GLOBALGOVT.staging.internationallabourorganization attributes
    GROUP BY 
        attributes.AGE_GROUP,
        attributes.ECONOMIC_ACTIVITY,
        attributes.ECONOMIC_CLASS,
        attributes.EMPLOYMENT_STATUS,
        attributes.GEOGRAPHY_TYPE,
        attributes.OCCUPATION,
        attributes.SEX,
        attributes.MEASURE,
        attributes.VARIABLE_NAME,
        attributes.DATE
)
SELECT 
    AGE_GROUP,
    ECONOMIC_ACTIVITY,
    ECONOMIC_CLASS,
    EMPLOYMENT_STATUS,
    GEOGRAPHY_TYPE,
    OCCUPATION,
    SEX,
    MEASURE,
    VARIABLE_NAME,
    DATE,
    Average_Value,
    Max_Value,
    Min_Value,
    (Average_Value - LAG(Average_Value) OVER (PARTITION BY AGE_GROUP, ECONOMIC_ACTIVITY, ECONOMIC_CLASS, EMPLOYMENT_STATUS, GEOGRAPHY_TYPE, OCCUPATION, SEX, MEASURE ORDER BY DATE)) AS Trend
FROM 
    AggregatedData
ORDER BY 
    AGE_GROUP, ECONOMIC_ACTIVITY, ECONOMIC_CLASS, EMPLOYMENT_STATUS, GEOGRAPHY_TYPE, OCCUPATION, SEX, MEASURE, DATE;

--Analyze WORLD_TRADE_ORGANIZATION_TIMESERIES for trade patterns by geography.//not done
WITH TradeData AS (
    SELECT 
        DATE,
        PARTNER_GEO_ID,
        REPORTING_GEO_ID,
        PRODUCT_SECTOR,
        WTO_INDICATOR,
        VARIABLE_NAME,
        MEASURE,
        FREQUENCY,
        SUM(VALUE) AS Total_Trade_Value
    FROM 
        GLOBALGOVT.staging.WORLDTRADEORGANIZATION
    WHERE 
        MEASUREMENT_TYPE = 'Trade Value' -- Focus on relevant measurement type
        AND FREQUENCY = 'Annual' -- Use annual data for meaningful trends
    GROUP BY 
        DATE, PARTNER_GEO_ID, REPORTING_GEO_ID, PRODUCT_SECTOR, WTO_INDICATOR, VARIABLE_NAME, MEASURE, FREQUENCY
),
TradeBalance AS (
    SELECT 
        DATE,
        REPORTING_GEO_ID,
        PARTNER_GEO_ID,
        SUM(CASE WHEN WTO_INDICATOR = 'Export' THEN Total_Trade_Value ELSE 0 END) AS Total_Exports,
        SUM(CASE WHEN WTO_INDICATOR = 'Import' THEN Total_Trade_Value ELSE 0 END) AS Total_Imports,
        SUM(CASE WHEN WTO_INDICATOR = 'Export' THEN Total_Trade_Value ELSE -Total_Trade_Value END) AS Trade_Balance
    FROM 
        TradeData
    GROUP BY 
        DATE, REPORTING_GEO_ID, PARTNER_GEO_ID
)
SELECT 
    DATE,
    REPORTING_GEO_ID,
    PARTNER_GEO_ID,
    Total_Exports,
    Total_Imports,
    Trade_Balance,
    CASE 
        WHEN Trade_Balance > 0 THEN 'Surplus'
        WHEN Trade_Balance < 0 THEN 'Deficit'
        ELSE 'Balanced'
    END AS Trade_Status
FROM 
    TradeBalance
ORDER BY 
    DATE, REPORTING_GEO_ID, PARTNER_GEO_ID;

--Utilize INTERNATIONAL_TRADE_ADMINISTRATION_TRADE_EVENTS_INDEX for industry-specific trade event insights.
create or replace table internationaltradeadministrationtradeeventsindex as 
select * from GLOBAL_GOVERNMENT.cybersyn.INTERNATIONAL_TRADE_ADMINISTRATION_TRADE_EVENTS_INDEX;

WITH IndustryEventSummary AS (
    SELECT 
        RELEVANT_INDUSTRIES,
        PRIMARY_VENUE_GEO_ID_COUNTRY,
        PRIMARY_VENUE_GEO_ID_STATE,
        COUNT(TRADE_EVENT_ID) AS Total_Events,
        MIN(START_DATE) AS First_Event_Date,
        MAX(END_DATE) AS Last_Event_Date,
        LISTAGG(DISTINCT TRADE_EVENT_TYPE, ', ') WITHIN GROUP (ORDER BY TRADE_EVENT_TYPE) AS Event_Types,
        LISTAGG(DISTINCT PRIMARY_VENUE_CITY, ', ') WITHIN GROUP (ORDER BY PRIMARY_VENUE_CITY) AS Cities_Hosting_Events
    FROM 
        GLOBALGOVT.staging.internationaltradeadministrationtradeeventsindex
    WHERE 
        RELEVANT_INDUSTRIES IS NOT NULL
    GROUP BY 
        RELEVANT_INDUSTRIES, PRIMARY_VENUE_GEO_ID_COUNTRY, PRIMARY_VENUE_GEO_ID_STATE
)
SELECT 
    RELEVANT_INDUSTRIES,
    PRIMARY_VENUE_GEO_ID_COUNTRY AS Country,
    PRIMARY_VENUE_GEO_ID_STATE AS State,
    Total_Events,
    First_Event_Date,
    Last_Event_Date,
    Event_Types,
    Cities_Hosting_Events
FROM 
    IndustryEventSummary
ORDER BY 
    RELEVANT_INDUSTRIES, Country, State;

--Location Data Enrichment
--Leverage GEOGRAPHY_INDEX for standardized geographic entities (e.g., ISO codes).
--Use GEOGRAPHY_RELATIONSHIPS to create hierarchy mappings (e.g., ZIP codes to counties).
WITH StandardizedGeography AS (
    SELECT
        g.GEO_ID,
        g.GEO_NAME,
        g.ISO_3166_2_CODE,
        g.ISO_ALPHA2,
        g.ISO_ALPHA3,
        g.ISO_NAME,
        g.ISO_NUMERIC_CODE
    FROM 
        GLOBAL_GOVERNMENT.cybersyn.GEOGRAPHY_INDEX g
),
GeographyHierarchy AS (
    SELECT
        gr.GEO_ID AS Source_GEO_ID,
        gr.GEO_NAME AS Source_GEO_NAME,
        gr.LEVEL AS Source_Level,
        gr.RELATED_GEO_ID AS Target_GEO_ID,
        gr.RELATED_GEO_NAME AS Target_GEO_NAME,
        gr.RELATED_LEVEL AS Target_Level,
        gr.RELATIONSHIP_TYPE
    FROM 
        GLOBAL_GOVERNMENT.cybersyn.GEOGRAPHY_RELATIONSHIPS gr
    WHERE 
        gr.RELATIONSHIP_TYPE IN ('Contains', 'BelongsTo') -- Filter for relevant relationship types
)
SELECT
    sg.GEO_ID AS Standardized_GEO_ID,
    sg.GEO_NAME AS Standardized_GEO_Name,
    sg.ISO_3166_2_CODE,
    sg.ISO_ALPHA2,
    sg.ISO_ALPHA3,
    sg.ISO_NAME,
    sg.ISO_NUMERIC_CODE,
    gh.Source_GEO_ID,
    gh.Source_GEO_NAME,
    gh.Source_Level,
    gh.Target_GEO_ID,
    gh.Target_GEO_NAME,
    gh.Target_Level,
    gh.RELATIONSHIP_TYPE
FROM 
    StandardizedGeography sg
LEFT JOIN 
    GeographyHierarchy gh
ON 
    sg.GEO_ID = gh.Source_GEO_ID
ORDER BY 
    sg.GEO_NAME, gh.Target_GEO_NAME;


--transportation and trade
create or replace table transporttrade as
SELECT 
    wto.DATE AS Trade_Date,
    wto.PARTNER_GEO_ID AS Partner_Country,
    wto.REPORTING_GEO_ID AS Reporting_Country,
    wto.UNIT AS Trade_Unit,
    wto.VALUE AS Trade_Value,
    wto.VARIABLE AS Trade_Variable,
    trans.DATE AS Transport_Date,
    trans.AIRCRAFT_CARRIER_ID AS Carrier_ID,
    trans.AIRCRAFT_ID AS Aircraft_ID,
    trans.ORIGIN_AIRPORT_ID AS Origin_Airport,
    trans.DESTINATION_AIRPORT_ID AS Destination_Airport,
    trans.SERVICE_CLASS AS Service_Class,
    attr.FREQUENCY AS Frequency,
    attr.MEASURE AS Measure,
    attr.UNIT AS Unit,
    attr.VARIABLE_NAME AS Variable_Name
FROM 
    GLOBAL_GOVERNMENT.cybersyn.WORLD_TRADE_ORGANIZATION_TIMESERIES wto
LEFT JOIN 
    GLOBAL_GOVERNMENT.cybersyn.US_DEPARTMENT_OF_TRANSPORTATION_ATTRIBUTES attr
ON 
    wto.VARIABLE = attr.VARIABLE
LEFT JOIN 
    GLOBAL_GOVERNMENT.cybersyn.US_DEPARTMENT_OF_TRANSPORTATION_TIMESERIES trans
ON 
    attr.VARIABLE = trans.VARIABLE
    AND wto.DATE = trans.DATE;

    --crime data with geoid
  create or replace table topcrime as
  SELECT 
    t.GEO_ID,
    g.GEO_NAME,
    t.TOTAL_CRIMES
FROM 
    GLOBALGOVT.staging.TOPCRIME t
JOIN 
    WEATHER__ENVIRONMENT.cybersyn.GEOGRAPHY_INDEX g
ON 
    t.GEO_ID = g.GEO_ID;

    --outliers
   -- Subquery to aggregate transport trade data by region and date
with quartile_val as (
    select 
        approx_percentile(VALUE, 0.25) as q1,
        approx_percentile(VALUE, 0.75) as q3
    from WORLDTRADEORGANIZATION
),
threshold_val as (
    select 
        q1,
        q3,
        (q3 - q1) as iqr,
        q1 - (1.5 * (q3 - q1)) as lower_bound_1,
        q3 + (1.5 * (q3 - q1)) as upper_bound_1,
        q1 - (2.5 * (q3 - q1)) as lower_bound_2,
        q3 + (2.5 * (q3 - q1)) as upper_bound_2
    from quartile_val
),
anomalies as (
    select 
        w.DATE,
        w.PARTNER_GEO_ID,
        w.REPORTING_GEO_ID,
        w.VALUE,
        t.lower_bound_1,
        t.upper_bound_1,
        t.lower_bound_2,
        t.upper_bound_2,
        case 
            when w.VALUE < t.lower_bound_2 or w.VALUE > t.upper_bound_2 then 'ANOMALY'
            else 'NORMAL'
        end as IS_ANOMALY
    from WORLDTRADEORGANIZATION as w
    join threshold_val as t
    on 1=1
    where w.DATE > '2000-01-01'
)
select 
    REPORTING_GEO_ID as COUNTRY,
    count(*) as ANOMALY_COUNT
from anomalies
where IS_ANOMALY = 'ANOMALY'
group by REPORTING_GEO_ID
order by ANOMALY_COUNT desc
limit 10;

with quartile_val as (
    select 
        approx_percentile(VALUE, 0.25) as q1,
        approx_percentile(VALUE, 0.75) as q3
    from WORLDTRADEORGANIZATION
),
threshold_val as (
    select 
        q1,
        q3,
        (q3 - q1) as iqr,
        q1 - (1.5 * (q3 - q1)) as lower_bound_1,
        q3 + (1.5 * (q3 - q1)) as upper_bound_1,
        q1 - (2.5 * (q3 - q1)) as lower_bound_2,
        q3 + (2.5 * (q3 - q1)) as upper_bound_2
    from quartile_val
)
select 
    w.DATE,
    w.PARTNER_GEO_ID,
    w.REPORTING_GEO_ID,
    w.VALUE,
    t.lower_bound_1,
    t.upper_bound_1,
    t.lower_bound_2,
    t.upper_bound_2,
    case 
        when w.VALUE < t.lower_bound_2 or w.VALUE > t.upper_bound_2 then 'ANOMALY'
        else 'NORMAL'
    end as IS_ANOMALY
from WORLDTRADEORGANIZATION as w
join threshold_val as t
on 1=1
where w.DATE > '2000-01-01'
order by w.DATE
limit 1000;