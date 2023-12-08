{% if var('BingAudiencePerformanceReport') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name("BingAudiencePerformanceReport_tbl_ptrn","BingAudiencePerformanceReport_tbl_exclude_ptrn") %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

    select 
    {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
    {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
    AccountId,		
    AccountName,		
    AccountNumber,		
    AccountStatus,		
    AdGroupId,		
    AdGroupName,		
    AdGroupStatus,		
    AudienceId,		
    AudienceName,		
    AudienceType,		
    AverageCpc,		
    AveragePosition	,		
    BidAdjustment,		
    CampaignId,		
    CampaignName,		
    CampaignStatus,		
    cast(Clicks as  BIGINT) as Clicks,		
    ConversionRate,		
    cast(Conversions as numeric) as Conversions,		
    CostPerConversion,		
    Ctr	,		
    cast(Impressions as BIGINT) as Impressions,		
    ReturnOnAdSpend	,		
    Revenue	,		
    RevenuePerConversion,		
    cast(Spend as numeric) as Spend	,		
    TargetingSetting,	
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








