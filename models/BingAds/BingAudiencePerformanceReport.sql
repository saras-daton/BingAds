{% if var('BingAudiencePerformanceReport') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}
{% if var('currency_conversion_flag') %}
--depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('BingAudiencePerformanceReport_tbl_ptrn'),
exclude=var('BingAudiencePerformanceReport_tbl_exclude_ptrn'),
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
        AccountId	,		
        AccountName	,		
        AccountNumber	,		
        AccountStatus	,		
        coalesce(AdGroupId,'NA') as AdGroupId	,		
        AdGroupName	,		
        AdGroupStatus	,		
        coalesce(AudienceId,'NA') as AudienceId	,		
        AudienceName	,		
        AudienceType	,		
        AverageCpc	,		
        AveragePosition	,		
        BidAdjustment	,		
        coalesce(CampaignId,'NA') as CampaignId	,		
        CampaignName	,		
        CampaignStatus	,		
        cast(Clicks as  BIGINT) as Clicks	,		
        ConversionRate	,		
        cast(Conversions as numeric) as Conversions	,		
        CostPerConversion	,		
        Ctr	,		
        cast(Impressions as BIGINT) as Impressions	,		
        ReturnOnAdSpend	,		
        Revenue	,		
        RevenuePerConversion	,		
        cast(Spend as numeric) as Spend	,		
        TargetingSetting	,	
        {{timezone_conversion("TimePeriod")}} as TimePeriod,
	   	{{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}}	
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('BingAudiencePerformanceReport_lookback') }},0) from {{ this }})
            --WHERE 1=1
            {% endif %}
        Qualify  DENSE_RANK() OVER (PARTITION BY adGroupId,campaignId,AudienceId,TimePeriod order by {{daton_batch_runtime()}} desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}		








