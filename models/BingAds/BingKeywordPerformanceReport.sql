{% if var('BingKeywordPerformanceReport') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}
{% if var('currency_conversion_flag') %}
--depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('BingKeywordPerformanceReport_tbl_ptrn'),
exclude=var('BingKeywordPerformanceReport_tbl_exclude_ptrn'),
database=var('raw_database')) %}

{% for i in relations %}
    {% if var('get_brandname_from_tablename_flag') %}
        {% set brand =replace(i,'`','').split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
    {% else %}
        {% set brand = var('default_brandname') %}
    {% endif %}

    {% if var('get_storename_from_tablename_flag') %}
        {% set store =replace(i,'`','').split('.')[2].split('_')[var('storename_position_in_tablename')] %}
    {% else %}
        {% set store = var('default_storename') %}
    {% endif %}


        select
        '{{brand|replace("`","")}}' as brand,
        '{{store|replace("`","")}}' as store,
        AccountName	,		
        AccountNumber	,		
        AccountId	,		
        {{timezone_conversion("TimePeriod")}} as TimePeriod,
        CampaignName	,		
        coalesce(CampaignId,'NA') as CampaignId	,		
        AdGroupName	,		
        coalesce(AdGroupId,'NA') as AdGroupId	,		
        Keyword	,		
        coalesce(KeywordId,'NA') as KeywordId	,		
        coalesce(AdId,'NA') as AdId	,		
        AdType	,		
        DestinationUrl	,		
        CurrentMaxCpc	,		
        CurrencyCode	,		
        coalesce(DeliveredMatchType,'NA') as DeliveredMatchType	,		
        AdDistribution	,		
        cast(Impressions as BIGINT) as Impressions	,		
        cast(Clicks as  BIGINT) as Clicks	,		
        Ctr	,		
        AverageCpc	,		
        cast(Spend as numeric) as Spend	,		
        AveragePosition	,		
        cast(Conversions as numeric)  as Conversions	,		
        ConversionRate	,		
        CostPerConversion	,		
        coalesce(BidMatchType,'NA') as BidMatchType	,		
        coalesce(DeviceType,'NA') as DeviceType	,		
        QualityScore	,		
        ExpectedCtr	,		
        AdRelevance	,		
        LandingPageExperience	,		
        Language	,		
        QualityImpact	,		
        CampaignStatus	,		
        AccountStatus	,		
        AdGroupStatus	,		
        KeywordStatus	,		
        Network	,		
        TopVsOther	,		
        coalesce(DeviceOS,'NA') as DeviceOS	,		
        Assists	,		
        Revenue	,		
        ReturnOnAdSpend	,		
        CostPerAssist	,		
        RevenuePerConversion	,		
        RevenuePerAssist	,		
        TrackingTemplate	,		
        CustomParameters	,		
        FinalUrl	,		
        FinalMobileUrl	,		
        FinalAppUrl	,		
        BidStrategyType	,		
        KeywordLabels	,		
        Mainline1Bid	,		
        MainlineBid	,		
        FirstPageBid,		
        {% if var('currency_conversion_flag') %}
            case when c.value is null then 1 else c.value end as exchange_currency_rate,
            case when c.from_currency_code is null then a.CurrencyCode else c.from_currency_code end as exchange_currency_code,
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            a.CurrencyCode as exchange_currency_code, 
        {% endif %}
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
           where a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('BingKeywordPerformanceReport_lookback') }},0) from {{ this }})
            --WHERE 1=1
            {% endif %}
        Qualify ROW_NUMBER() OVER (PARTITION BY CampaignId,adGroupId,KeywordId,AdId,DeliveredMatchType,BidMatchType,DeviceOS,TimePeriod order by a.{{daton_batch_runtime()}} desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}		