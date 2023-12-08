{% if var('BingCampaignPerformanceReport') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name("BingCampaignPerformanceReport_tbl_ptrn","BingCampaignPerformanceReport_tbl_exclude_ptrn") %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

    select 
    {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
    {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
    AccountId,		
    AccountName,		
    AccountNumber,		
    AccountStatus,		
    AdDistribution,		
    BidMatchType,		
    BudgetName,		
    BudgetStatus,		
    CampaignId,		
    CampaignLabels,		
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
    DeviceOS,		
    DeviceType,	
    {{timezone_conversion("TimePeriod")}} as TimePeriod,
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
            where a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('BingCampaignPerformanceReport_lookback') }},0) from {{ this }})
            --WHERE 1=1
        {% endif %}
    qualify ROW_NUMBER() OVER (PARTITION BY  CampaignId,DeliveredMatchType,BidMatchType,DeviceOS,date(TimePeriod) order by {{daton_batch_runtime()}} desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}