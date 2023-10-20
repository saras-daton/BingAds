{% if var('BingAccountPerformanceReport') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}
{% if var('currency_conversion_flag') %}
--depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('BingAccountPerformanceReport_tbl_ptrn'),
exclude=var('BingAccountPerformanceReport_tbl_exclude_ptrn'),
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
        AdDistribution	,		
        cast(Impressions as int64) as Impressions	,		
        cast(Clicks as int64) as Clicks	,		
        Ctr	,		
        AverageCpc	,		
        cast(Spend as numeric) as Spend	,		
        AveragePosition	,		
        ConversionRate	,		
        cast(Conversions as numeric) as Conversions	,		
        CostPerAssist	,		
        AccountStatus	,		
        COALESCE(BidMatchType,'NA') as BidMatchType	,		
        CurrencyCode	,		
        CustomerId	,		
        CustomerName	,		
        COALESCE(DeviceOS, 'NA') as DeviceOS,		
        COALESCE(DeliveredMatchType, 'NA') as DeliveredMatchType	,		
        COALESCE(DeviceType, 'NA') as DeviceType	,		
        COALESCE(Network,'NA') as Network	,		
        COALESCE(TopVsOther,'NA')  as TopVsOther	,		
        CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cast(TimePeriod as timestamp)") }} as {{ dbt.type_timestamp() }}) as TimePeriod,		
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
        from {{i}} a  
            {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} c on date(TimePeriod) = c.date and a.CurrencyCode = c.to_currency_code
            {% endif%}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('BingAccountPerformanceReport_lookback') }},0) from {{ this }})
            --WHERE 1=1
            {% endif %}
        qualify DENSE_RANK() OVER (PARTITION BY  AccountNumber,TopVsOther,Network,DeliveredMatchType,BidMatchType,DeviceOS,DeviceType,date(TimePeriod) order by {{daton_batch_runtime()}} desc) = 1
    
    {% if not loop.last %} union all {% endif %}
    {% endfor %}