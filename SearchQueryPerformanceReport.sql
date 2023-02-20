{% if var('table_partition_flag') %}
{{config( 
    materialized='incremental', 
    incremental_strategy='merge', 
    partition_by = { 'field': 'TimePeriod', 'data_type': 'date' },
    cluster_by = ['AccountNumber','AdId'], 
    unique_key = ['AccountNumber','AdId','AdType','DeviceOS','TopVsOther','Network','DeviceType','TimePeriod','KeywordId','SearchQuery','BidMatchType'])}}
{% else %}
{{config( 
    materialized='incremental', 
    incremental_strategy='merge', 
    unique_key = ['AccountNumber','AdId','AdType','DeviceOS','TopVsOther','Network','DeviceType','TimePeriod','KeywordId','SearchQuery','BidMatchType'])}}
{% endif %}

{% if is_incremental() %}
{%- set max_loaded_query -%}
SELECT coalesce(MAX(_daton_batch_runtime) - 2592000000,0) FROM {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%}
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}

{% set table_name_query %}
select concat('`', table_catalog,'.',table_schema, '.',table_name,'`') as tables 
from {{ var('raw_projectid') }}.{{ var('raw_dataset') }}.INFORMATION_SCHEMA.TABLES
where lower(table_name) like '%search_query_performance_report' 
{% endset %}  



{% set results = run_query(table_name_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

{% if var('timezone_conversion_flag') %}
    {% set hr = var('timezone_conversion_hours') %}
{% endif %}

{% for i in results_list %}
    {% if var('brand_consolidation_flag') %}
        {% set id =i.split('.')[2].split('_')[var('brand_name_position')] %}
    {% else %}
        {% set id = var('brand_name') %}
    {% endif %}

    SELECT * except(row_num)
    From (
        select '{{id}}' as brand,
        AccountId,
        AccountName,
        COALESCE(AccountNumber,'') as AccountNumber,
        AccountStatus ,
        AdGroupCriterionId ,
        AdGroupId ,
        AdGroupName ,
        AdGroupStatus ,
        COALESCE(AdId,'') as AdId,
        AdStatus ,
        COALESCE(AdType,'') as AdType,
        Assists ,
        AverageCpc ,
        AveragePosition ,
        COALESCE(BidMatchType,'') as BidMatchType,
        CampaignId ,
        CampaignName  ,
        CampaignStatus ,
        CampaignType  ,
        Clicks ,
        ConversionRate ,
        Conversions ,
        CostPerAssist ,
        CostPerConversion ,
        Ctr ,
        CustomerId ,
        CustomerName ,
        coalesce(DeliveredMatchType,'') as DeliveredMatchType,
        DestinationUrl ,
        coalesce(DeviceOS,'') as DeviceOS,
        coalesce(DeviceType,'') as DeviceType,
        Impressions ,
        Keyword ,
        coalesce(KeywordId,'') as KeywordId,
        KeywordStatus ,
        Language ,
        coalesce(Network,'') as Network,
        ReturnOnAdSpend ,
        Revenue ,
        RevenuePerAssist ,
        RevenuePerConversion ,
        COALESCE(SearchQuery,'') as SearchQuery,
        Spend ,
        cast(TimePeriod as date)TimePeriod ,
        coalesce(TopVsOther,'') as TopVsOther,
        _daton_user_id ,
        _daton_batch_runtime ,
        _daton_batch_id,
        {% if var('timezone_conversion_flag') %}
            DATETIME_ADD(TIMESTAMP_MILLIS(cast(_daton_batch_runtime as int)), INTERVAL {{hr}} HOUR ) as _edm_eff_strt_ts,
        {% else %}
            TIMESTAMP_MILLIS(cast(_daton_batch_runtime as int)) _edm_eff_strt_ts,
        {% endif %}
        null as _edm_eff_end_ts,
        unix_micros(current_timestamp()) as _edm_runtime,
        DENSE_RANK() OVER (PARTITION BY TimePeriod,AccountNumber,AdId,TopVsOther,DeviceOS,Network,DeviceType,TimePeriod,KeywordId,SearchQuery,BidMatchType order by _daton_batch_runtime desc) row_num
        from {{i}} a    
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE _daton_batch_runtime  >= {{max_loaded}}
            --WHERE 1=1
            {% endif %}
        )
    where row_num =1 
    {% if not loop.last %} union all {% endif %}
{% endfor %}