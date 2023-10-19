{% if var('BingSearchQueryPerformanceReport') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}
{% if var('currency_conversion_flag') %}
--depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('BingSearchQueryPerformanceReport_tbl_ptrn'),
exclude=var('BingSearchQueryPerformanceReport_tbl_exclude_ptrn'),
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
    
        select '{{brand}}' as brand,
        '{{store}}' as store,
        AccountId,
        AccountName,
        coalesce(AccountNumber,'NA') as AccountNumber,
        AccountStatus ,
        AdGroupCriterionId ,
        AdGroupId ,
        AdGroupName ,
        AdGroupStatus ,
        coalesce(AdId,'NA') as AdId,
        AdStatus ,
        coalesce(AdType,'NA') as AdType,
        Assists ,
        AverageCpc ,
        AveragePosition ,
        COALESCE(BidMatchType,'NA') as BidMatchType,
        CampaignId ,
        CampaignName  ,
        CampaignStatus ,
        CampaignType  ,
        cast(Clicks as int64) as Clicks ,
        ConversionRate ,
        cast(Conversions as numeric) as Conversions,
        CostPerAssist ,
        CostPerConversion ,
        Ctr ,
        CustomerId ,
        CustomerName ,
        coalesce(DeliveredMatchType,'NA') as DeliveredMatchType,
        DestinationUrl ,
        coalesce(DeviceOS,'NA') as DeviceOS,
        coalesce(DeviceType,'NA') as DeviceType,
        cast(Impressions as int64) as Impressions ,
        Keyword ,
        coalesce(KeywordId,'NA') as KeywordId,
        KeywordStatus ,
        Language ,
        coalesce(Network,'NA') as Network,
        ReturnOnAdSpend ,
        Revenue ,
        RevenuePerAssist ,
        RevenuePerConversion ,
        COALESCE(SearchQuery,'NA') as SearchQuery,
        cast(Spend as numeric) as Spend ,
        CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cast(TimePeriod as timestamp)") }} as {{ dbt.type_timestamp() }}) as TimePeriod,
        coalesce(TopVsOther,'NA') as TopVsOther,
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,            
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        from {{i}} a    
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE {{daton_batch_runtime()}}  >= (SELECT coalesce(MAX(_daton_batch_runtime) - 2592000000,0) FROM {{ this }})
            --WHERE 1=1
            {% endif %}
        Qualify ROW_NUMBER() OVER (PARTITION BY AccountNumber,AdId,TopVsOther,DeviceOS,Network,DeviceType,KeywordId,SearchQuery,BidMatchType,DeliveredMatchType,TimePeriod order by {{daton_batch_runtime()}} desc) = 1
            {% if not loop.last %} union all {% endif %}
            {% endfor %}
