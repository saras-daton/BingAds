# Bing Ads Data Unification

This dbt package is for the Bing Ads data unification Ingested by [Daton](https://sarasanalytics.com/daton/). [Daton](https://sarasanalytics.com/daton/) is the Unified Data Platform for Global Commerce with 100+ pre-built connectors and data sets designed for accelerating the eCommerce data and analytics journey by [Saras Analytics](https://sarasanalytics.com).

### Supported Datawarehouses:
- BigQuery
- Snowflake

#### Typical challanges with raw data are:
- Array/Nested Array columns which makes queries for Data Analytics complex
- Data duplication due to look back period while fetching report data from Bing Ads
- Seperate tables at marketplaces/Store, brand, account level for same kind of report/data feeds

By doing Data Unification the above challenges can be overcomed and simplifies Data Analytics. 
As part of Data Unification, the following funtions are performed:
- Consolidation - Different marketplaces/Store/account & different brands would have similar raw Daton Ingested tables, which are consolidated into one table with column distinguishers brand & store
- Deduplication - Based on primary keys, the data is De-duplicated and the latest records are only loaded into the consolidated stage tables
- Incremental Load - Models are designed to include incremental load which when scheduled would update the tables regularly
- Standardization -
	- Currency Conversion (Optional) - Raw Tables data created at Marketplace/Store/Account level may have data in local currency of the corresponding marketplace/store/account. Values that are in local currency are standardized by converting to desired currency using Daton Exchange Rates data.
	  Prerequisite - Exchange Rates connector in Daton needs to be present - Refer [this](https://github.com/saras-daton/currency_exchange_rates)
	- Time Zone Conversion (Optional) - Raw Tables data created at Marketplace/Store/Account level may have data in local timezone of the corresponding marketplace/store/account. DateTime values that are in local timezone are standardized by converting to specified timezone using input offset hours.

#### Prerequisite 
Daton Integrations for  
- Bing Ads 
- Exchange Rates(Optional, if currency conversion is not required)

*Note:* 
*Please select 'Do Not Unnest' option while setting up Daton Integrataion*

# Configuration 

## Required Variables

This package assumes that you have an existing dbt project with a BigQuery/Snowflake profile connected & tested. Source data is located using the following variables which must be set in your `dbt_project.yml` file.
```yaml
vars:
    raw_database: "your_database"
    raw_schema: "your_schema"
```

## Setting Target Schema

Models will be create unified tables under the schema (<target_schema>_stg_bingads). In case, you would like the models to be written to the target schema or a different custom schema, please add the following in the dbt_project.yml file.

```yaml
models:
  bing_ads:
    +schema: custom_schema_extension
```

## Optional Variables

Package offers different configurations which must be set in your `dbt_project.yml` file. These variables can be marked as True/False based on your requirements. Details about the variables are given below.

### Currency Conversion 

To enable currency conversion, which produces two columns - exchange_currency_rate & exchange_currency_code, please mark the currency_conversion_flag as True. By default, it is False.
Prerequisite - Daton Exchange Rates Integration

Example:
```yaml
vars:
    currency_conversion_flag: True
```

### Timezone Conversion 

To enable timezone conversion, which converts the datetime columns from local timezone to given timezone, please mark the timezone_conversion_flag f as True in the dbt_project.yml file, by default, it is False
Additionally, you need to provide offset hours for each raw table

Example:
```yaml
vars:
timezone_conversion_flag: False
raw_table_timezone_offset_hours: {
    "Bing.Ads.Brand_US_BingAds_180494538_account_performance_report" : -6,
    "Bing.Ads.Brand_US_BingAds_180494538_ad_extension_by_keyword_report" : -6,
    "Bing.Ads.Brand_US_BingAds_180494538_ad_extension_detail_report" : -6,
    "Bing.Ads.Brand_US_BingAds_180494538_audience_performance_report" : -6,
    "Bing.Ads.Brand_US_BingAds_180494538_campaign_performance_report" : -6,
    "Bing.Ads.Brand_US_BingAds_180494538_conversion_performance_report" : -6,
    "Bing.Ads.Brand_US_BingAds_180494538_goals_and_funnels_report" : -6,
    "Bing.Ads.Brand_US_BingAds_180494538_keyword_performance_report" : -6,
    "Bing.Ads.Brand_US_BingAds_180494538_ad_performance_report" : -6,
    "Bing.Ads.Brand_US_BingAds_180494538_search_query_performance_report" : -6
}

Note : Here, '-6' is given as the offset hour as per the time difference between UTC and US timezones. Provide the offset hour accordingly for each table based on your data.

```
### Table Exclusions

If you need to exclude any of the models, declare the model names as variables and mark them as False. Refer the table below for model details. By default, all tables are created.

Example:
```yaml
vars:
BingAdPerformanceReport: False
```

## Models

This package contains models from the Bing Ads API which includes reports on {{sales, margin, inventory, product}}. The primary outputs of this package are described below.

| **Category**                 | **Model**  | **Description** |
| ------------------------- | ---------------| ----------------------- |
|Performance | [BingAccountPerformanceReport](models/BingAds/BingAccountPerformanceReport.sql)  | This report provides long-term account performance and trends using impressions, clicks, spend, and average cost per click for individual accounts |
|AdExtension | [BingAdExtensionByKeywordReport](models/BingAds/BingAdExtensionByKeywordReport.sql)  |This report helps compare how well different versions of your ad extensions are performing for each keyword. |
|AdExtension | [BingAdExtensionDetailReport](models/BingAds/BingAdExtensionDetailReport.sql)  | This report provides a detailed and closer look at how each ad extension is performing based on click type for ads  |s
|Performance | [BingAdPerformanceReport](models/BingAds/BingAdPerformanceReport.sql)  | A report on ads performance based on impressions, clicks, spend, and average cost per click for each ad |
|Performance | [BingAudiencePerformanceReport](models/BingAds/BingAudiencePerformanceReport.sql)  |This report helps compare delivery performance statistics by audience, ad group, campaign, or account attributes. |
|Performance | [BingCampaignPerformanceReport](models/BingAds/BingCampaignPerformanceReport.sql)  | This report helps view high-level performance statistics and quality attributes for each campaign or account. |
|Performance | [BingConversionPerformanceReport](models/BingAds/BingConversionPerformanceReport.sql)  |This report helps understand which campaigns and keywords are leading an audience to complete a purchase or other conversion action |
|Performance | [BingGoalsAndFunnelsReport](models/BingAds/BingGoalsAndFunnelsReport.sql)  | This report helps discover whether your audience completes each step through the land, browse, prospect, and conversion pages of your website |
|Performance | [BingKeywordPerformanceReport](models/BingAds/BingKeywordPerformanceReport.sql)  | This report depicts which keywords are performing well and those that are not |
|Performance | [BingSearchQueryPerformanceReport](models/BingAds/BingSearchQueryPerformanceReport.sql)  | A report on keywords search terms that have triggered your ads |


### For details about default configurations for Table Primary Key columns, Partition columns, Clustering columns, please refer the properties.yaml used for this package as below. 
	You can overwrite these default configurations by using your project specific properties yaml.
```yaml
version: 2
models:
  - name: BingAccountPerformanceReport
    description: This report provides long-term account performance and trends using impressions, clicks, spend, and average cost per click for individual accounts
    config : 
      materialized : incremental
      incremental_strategy : merge 
      unique_key : ['AccountNumber','TopVsOther','BidMatchType','Network','BidMatchType','DeliveredMatchType','DeviceOS','DeviceType','impressions']
      partition_by : { 'field': 'TimePeriod', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by : ['AccountNumber']


  - name: BingAdExtensionByKeywordReport
    description: This report helps compare how well different versions of your ad extensions are performing for each keyword.
    config:
      materialized : incremental
      incremental_strategy : merge 
      unique_key : ['AccountNumber','TopVsOther','BidMatchType','Network','DeliveredMatchType','DeviceOS','DeviceType']
      partition_by : { 'field': 'TimePeriod', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by : ['AccountNumber']
 
  - name: BingAdExtensionDetailReport
    description: This report provides a detailed and closer look at how each ad extension is performing based on click type for ads 
    config:
      materialized : incremental
      incremental_strategy : merge 
      unique_key : ['AccountName','AdExtensionId','adGroupId','AdId','CampaignId','DeliveredMatchType','DeviceOS','DeviceType','Network']
      partition_by : { 'field': 'TimePeriod', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by : ['AdExtensionId']

  - name: BingAdPerformanceReport
    description: A report on ads performance based on impressions, clicks, spend, and average cost per click for each ad
    config: 
      materialized : incremental 
      incremental_strategy : merge 
      unique_key : ['AccountNumber','AdId','AdType','TopVsOther','BidMatchType','Network','DeliveredMatchType','DeviceOS','DeviceType'] 
      partition_by : { 'field': 'TimePeriod', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by : ['AccountNumber','AdId']

  - name: BingAudiencePerformanceReport
    description: This report helps compare delivery performance statistics by audience, ad group, campaign, or account attributes.
    config:
      materialized : incremental
      incremental_strategy : merge 
      unique_key : ['adGroupId','campaignId','AudienceId']
      partition_by : { 'field': 'TimePeriod', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by : ['AudienceId']
 
  - name: BingCampaignPerformanceReport
    description: This report helps view high-level performance statistics and quality attributes for each campaign or account.
    config:
      materialized : incremental
      incremental_strategy : merge 
      unique_key : ['CampaignId','BidMatchType','DeliveredMatchType','DeviceOS','DeviceType']
      partition_by : { 'field': 'TimePeriod', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by : ['CampaignId']
 
  - name: BingConversionPerformanceReport
    description: This report helps understand which campaigns and keywords are leading an audience to complete a purchase or other conversion action
    config:
      materialized : incremental
      incremental_strategy : merge
      unique_key : ['AdGroupId','KeywordId','DeviceType','impressions']
      partition_by : { 'field': 'TimePeriod', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by : ['AdGroupId']
 
  - name: BingGoalsAndFunnelsReport
    description: This report helps discover whether your audience completes each step through the land, browse, prospect, and conversion pages of your website
    config:
      materialized : incremental
      incremental_strategy : merge 
      unique_key : ['campaignId','KeywordId','GoalId','DeviceOS','DeviceType']
      partition_by : { 'field': 'TimePeriod', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by : ['campaignId']
 
  - name: BingKeywordPerformanceReport
    description: This report depicts which keywords are performing well and those that are not
    config:
      materialized : incremental
      incremental_strategy : merge 
      unique_key : ['CampaignId','adGroupId','KeywordId','AdId','DeliveredMatchType','BidMatchType','DeviceOS','DeviceType']
      partition_by : { 'field': 'TimePeriod', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by : ['CampaignId']

  - name: BingSearchQueryPerformanceReport
    description: A report on keywords, what your audience is searching for when your ads are shown
    config:
      materialized : incremental 
      incremental_strategy : merge 
      unique_key : ['AccountNumber','AdId','AdType','DeviceOS','TopVsOther','Network','DeviceType','KeywordId','SearchQuery','BidMatchType','DeliveredMatchType']
      partition_by : { 'field': 'TimePeriod', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by : ['AccountNumber','AdId']


```



## Resources:
- Have questions, feedback, or need [help](https://calendly.com/srinivas-janipalli/30min)? Schedule a call with our data experts or email us at info@sarasanalytics.com.
- Learn more about Daton [here](https://sarasanalytics.com/daton/).
- Refer [this](https://youtu.be/6zDTbM6OUcs) to know more about how to create a dbt account & connect to {{Bigquery/Snowflake}}
