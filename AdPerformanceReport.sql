    -- depends_on: {{ ref('ExchangeRates') }}
{% if var('table_partition_flag') %}
{{config( 
    materialized='incremental', 
    incremental_strategy='merge', 
    partition_by = { 'field': 'TimePeriod', 'data_type': 'date' },
    cluster_by = ['AccountNumber','AdId'], 
    unique_key = ['AccountNumber','AdId','AdType','TopVsOther','BidMatchType','Network','DeliveredMatchType','DeviceOS','DeviceType','TimePeriod'])}}
{% else %}
{{config( 
    materialized='incremental', 
    incremental_strategy='merge', 
    unique_key = ['AccountNumber','AdId','AdType','TopVsOther','BidMatchType','Network','DeliveredMatchType','DeviceOS','DeviceType','TimePeriod'])}}
{% endif %}

-- depends_on: {{ref('ExchangeRates')}}

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
where lower(table_name) like '%ad_performance_report' 
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
        {% set brand =i.split('.')[2].split('_')[var('brand_name_position')] %}
    {% else %}
        {% set brand = var('brand_name') %}
    {% endif %}

    SELECT * except(row_num)
    From (
        select
        '{{brand}}' as brand,
        AccountId,
        AccountName,
        COALESCE(AccountNumber,'') as AccountNumber,
        AccountStatus,
        AdDescription 
        AdDistribution,
        AdGroupId,
        AdGroupName,
        AdGroupStatus,
        COALESCE(AdId,'') as AdId,
        AdLabels,
        AdStatus,
        AdTitle,
        COALESCE(AdType,'') as AdType,
        Assists,
        AverageCpc,
        AveragePosition,
        COALESCE(BidMatchType,'') as BidMatchType,
        BusinessName 
        CampaignId,
        CampaignName,
        CampaignStatus,
        Clicks,
        ConversionRate,
        Conversions,
        CostPerAssist,
        CostPerConversion,
        Ctr,
        CurrencyCode,
        CustomerId,
        CustomerName,
        CustomParameters,
        coalesce(DeliveredMatchType,'') as DeliveredMatchType,
        DestinationUrl,
        coalesce(DeviceOS,'') as DeviceOS,
        coalesce(DeviceType,'') as DeviceType,
        DisplayUrl,
        FinalAppUrl,
        FinalMobileUrl,
        FinalUrl 
        Headline,
        Impressions,
        Language,
        LongHeadline,
        coalesce(Network,'') as Network,
        Path1,
        Path2,
        ReturnOnAdSpend,
        Revenue,
        RevenuePerAssist,
        RevenuePerConversion,
        Spend,
        cast(TimePeriod as date)TimePeriod,
        TitlePart1,
        TitlePart2,
        coalesce(TopVsOther,'') as TopVsOther,
        TrackingTemplate,
        {% if var('currency_conversion_flag') %}
            c.value as exchange_currency_rate,
            c.from_currency_code as exchange_currency_code, 
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            cast(null as string) as exchange_currency_code, 
        {% endif %}
        a._daton_user_id,
        a._daton_batch_runtime,
        a._daton_batch_id,
        {% if var('timezone_conversion_flag') %}
            DATETIME_ADD(TIMESTAMP_MILLIS(cast(a._daton_batch_runtime as int)), INTERVAL {{hr}} HOUR ) as _edm_eff_strt_ts,
        {% else %}
            TIMESTAMP_MILLIS(cast(a._daton_batch_runtime as int)) _edm_eff_strt_ts,
        {% endif %}
        null as _edm_eff_end_ts,
        unix_micros(current_timestamp()) as _edm_runtime,
        DENSE_RANK() OVER (PARTITION BY TimePeriod ,AccountNumber,AdId,TopVsOther,Network,DeliveredMatchType,BidMatchType,DeviceOS order by a._daton_batch_runtime desc) row_num
        from {{i}} a  
            {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} c on date(TimePeriod) = c.date and a.CurrencyCode = c.to_currency_code
            {% endif%}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE a._daton_batch_runtime  >= {{max_loaded}}
            --WHERE 1=1
            {% endif %}
        )
    where row_num =1 
    {% if not loop.last %} union all {% endif %}
{% endfor %}