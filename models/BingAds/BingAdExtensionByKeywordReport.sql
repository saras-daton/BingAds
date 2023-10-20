{% if var('BingAdExtensionByKeywordReport') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}
{% if var('currency_conversion_flag') %}
--depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('BingAdExtensionByKeywordReport_tbl_ptrn'),
exclude=var('BingAdExtensionByKeywordReport_tbl_exclude_ptrn'),
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

        {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours') %}
            {% set hr = var('raw_table_timezone_offset_hours')[i] %}
        {% else %}
            {% set hr = 0 %}
        {% endif %}

        select
        '{{brand}}' as brand,
        '{{store}}' as store,
        AccountName	,		
        COALESCE(AccountNumber,'NA') as AccountNumber	,		
        AccountId	,		
        AccountStatus	,		
        AdExtensionId	,		
        AdExtensionType	,		
        AdExtensionVersion	,		
        AdGroupName	,		
        AdGroupId	,		
        AdGroupStatus	,		
        AverageCpc	,		
        COALESCE(BidMatchType,'NA') as BidMatchType	,		
        CampaignId	,		
        CampaignName	,		
        CampaignStatus	,		
        cast(Clicks as int64) as Clicks	,		
        ClickType	,		
        ConversionRate	,		
        cast(Conversions as numeric) as Conversions	,		
        CostPerAssist	,		
        CostPerConversion	,		
        Ctr	,		
        COALESCE(DeliveredMatchType,'NA') as DeliveredMatchType	,		
        COALESCE(DeviceOS,'NA') as DeviceOS	,		
        COALESCE(DeviceType,'NA') as DeviceType	,		
        cast(Impressions as int64) as Impressions	,		
        Keyword	,		
        KeywordId	,		
        KeywordStatus	,		
        COALESCE(Network,'NA') as Network,		
        ReturnOnAdSpend	,		
        Revenue	,		
        RevenuePerAssist	,	

        RevenuePerConversion	,		
        cast (Spend as numeric) as Spend	,		
        CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cast(TimePeriod as timestamp)") }} as {{ dbt.type_timestamp() }}) as TimePeriod,		
        COALESCE(TopVsOther,'NA') as TopVsOther	,		
        TotalClicks	,		
	   	{{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        from {{i}}	
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('BingAdExtensionByKeywordReport_lookback') }},0) from {{ this }})
            --WHERE 1=1
            {% endif %}
        qualify row_number() OVER (PARTITION BY AccountNumber,TopVsOther,Network,DeliveredMatchType,BidMatchType,DeviceOS,DeviceType,date(TimePeriod) order by {{daton_batch_runtime()}} desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}	
