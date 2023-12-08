
{% if var('BingAdPerformanceReport') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name("BingAdPerformanceReport_tbl_ptrn","BingAdPerformanceReport_tbl_exclude_ptrn") %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

    select 
    {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
    {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
    AccountId,
    AccountName,
    AccountNumber,
    AccountStatus,
    AdDescription 
    AdDistribution,
    AdGroupId,
    AdGroupName,
    AdGroupStatus,
    AdId,
    AdLabels,
    AdStatus,
    AdTitle,
    AdType,
    Assists,
    AverageCpc,
    AveragePosition,
    BidMatchType,
    BusinessName, 
    CampaignId,
    CampaignName,
    CampaignStatus,
    cast(Clicks as BIGINT) as Clicks,
    ConversionRate,
    cast(Conversions as numeric) as Conversions,
    CostPerAssist,
    CostPerConversion,
    Ctr,
    CurrencyCode,
    CustomerId,
    CustomerName,
    CustomParameters,
    DeliveredMatchType,
    DestinationUrl,
    DeviceOS,
    DeviceType,
    DisplayUrl,
    FinalAppUrl,
    FinalMobileUrl,
    FinalUrl 
    Headline,
    cast(Impressions as BIGINT) as Impressions,
    Language,
    LongHeadline,
    Network,
    Path1,
    Path2,
    ReturnOnAdSpend,
    Revenue,
    RevenuePerAssist,
    RevenuePerConversion,
    cast(Spend as numeric) as Spend,
    {{timezone_conversion("TimePeriod")}} as TimePeriod,		
    TitlePart1,
    TitlePart2,
    TopVsOther,
    TrackingTemplate,
    {#/*Currency_conversion as exchange_rates alias can be differnt we have value and from_currency_code*/#}
    {{ currency_conversion('c.value', 'c.from_currency_code', 'CurrencyCode') }},
    a.{{daton_user_id()}} as _daton_user_id,
    a.{{daton_batch_runtime()}} as _daton_batch_runtime,
    a.{{daton_batch_id()}} as _daton_batch_id,
    current_timestamp() as _last_updated,
    '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
    from {{i}} a  
        {% if var('currency_conversion_flag') %}
            left join {{ref('ExchangeRates')}} c on date(TimePeriod) = c.date and a.CurrencyCode = c.to_currency_code
        {% endif%}
        {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('BingAdPerformanceReport_lookback') }},0) from {{ this }})
            --WHERE 1=1
        {% endif %}
    Qualify  ROW_NUMBER() OVER (PARTITION BY AccountNumber,AdId,AdTitle,AdType,DeviceOS,devicetype,DeliveredMatchType,BidMatchType,network,TopVsOther,TimePeriod order by a.{{daton_batch_runtime()}} desc) = 1 
{% if not loop.last %} union all {% endif %}
{% endfor %}