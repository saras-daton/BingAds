{% if var('BingKeywordPerformanceReport') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
 --depends_on: {{ ref('ExchangeRates') }}
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
    {{set_table_name('%keyword_performance_report')}}    
    {% endset %}  

    {% set results = run_query(table_name_query) %}
    {% if execute %}
        {# Return the first column #}
        {% set results_list = results.columns[0].values() %}
        {% set tables_lowercase_list = results.columns[1].values() %}
    {% else %}
        {% set results_list = [] %}
        {% set tables_lowercase_list = [] %}
    {% endif %}


    {% for i in results_list %}
        {% if var('get_brandname_from_tablename_flag') %}
             {% set brand =i.split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
        {% else %}
             {% set brand = var('default_brandname') %}
        {% endif %}

        {% if var('get_storename_from_tablename_flag') %}
            {% set store =i.split('.')[2].split('_')[var('storename_position_in_tablename')] %}
        {% else %}
            {% set store = var('default_storename') %}
        {% endif %}

        {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours') %}
            {% set hr = var('raw_table_timezone_offset_hours')[i] %}
        {% else %}
            {% set hr = 0 %}
        {% endif %}


        SELECT * {{exclude()}}(row_num)
    From (
        select
        '{{brand}}' as brand,
        '{{store}}' as store,
        AccountName	,		
        AccountNumber	,		
        AccountId	,		
        CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cast(TimePeriod as timestamp)") }} as {{ dbt.type_timestamp() }}) as TimePeriod,		
        CampaignName	,		
        CampaignId	,		
        AdGroupName	,		
        AdGroupId	,		
        Keyword	,		
        KeywordId	,		
        AdId	,		
        AdType	,		
        DestinationUrl	,		
        CurrentMaxCpc	,		
        CurrencyCode	,		
        DeliveredMatchType	,		
        AdDistribution	,		
        Impressions	,		
        Clicks	,		
        Ctr	,		
        AverageCpc	,		
        Spend	,		
        AveragePosition	,		
        Conversions	,		
        ConversionRate	,		
        CostPerConversion	,		
        BidMatchType	,		
        DeviceType	,		
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
        DeviceOS	,		
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
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        ROW_NUMBER() OVER (PARTITION BY CampaignId,adGroupId,KeywordId,AdId,DeliveredMatchType,BidMatchType,DeviceOS,TimePeriod order by {{daton_batch_runtime()}} desc) row_num
        from {{i}} a  
            {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} c on date(TimePeriod) = c.date and a.CurrencyCode = c.to_currency_code
            {% endif%}	
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
            --WHERE 1=1
            {% endif %}
        )
    where row_num =1 
    {% if not loop.last %} union all {% endif %}
{% endfor %}		