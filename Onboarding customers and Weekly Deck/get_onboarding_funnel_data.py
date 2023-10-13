from get_data import get_data_from_snowflake
import pandas as pd
import warnings
from datetime import date, timedelta
warnings.filterwarnings('ignore')


def get_results(daily_update=None,weekly_update=None):

    if daily_update:
        df_onboarding = get_data_from_snowflake("""SELECT application_status,
        detailed_application_status,
        application_status_category,
        COUNT(*)
        FROM credit.application.application_summary
        WHERE testing_stage = 'Rollout'
            AND application_recency = 1
            AND (cast(profile_creation_ts as date) = {})
        GROUP BY 1,2,3
        ORDER BY 3,1
        ;""".format(daily_update))
        df_subinfo = get_data_from_snowflake("""SELECT application_status,
        detailed_application_status,
        application_status_category,
        SUM_SALARY_INCOME_TXN_0_365,
        TOTAL_BALANCE,
        NEG_BAL_OCCURENCES_30D,
        cast(profile_creation_ts as date),
        COUNT(*)
        FROM credit.application.application_summary
        WHERE testing_stage = 'Rollout'
            AND application_recency = 1
            AND (cast(profile_creation_ts as date) = {})
            AND (DETAILED_APPLICATION_STATUS = 'Income reported is insufficient' or lower(DETAILED_APPLICATION_STATUS) = 'number of recent negative balance occurrences is too high' or lower(DETAILED_APPLICATION_STATUS) = 'total connected account balance is too low')
        GROUP BY 1,2,3,4,5,6,7
        ORDER BY 3,1
        ;""".format(daily_update))
    else:
        df_onboarding = get_data_from_snowflake("""SELECT application_status,
        detailed_application_status,
        application_status_category,
        COUNT(*)
        FROM credit.application.application_summary
        WHERE testing_stage = 'Rollout'
            AND application_recency = 1
            AND (cast(profile_creation_ts as date) between {} and {})
        GROUP BY 1,2,3
        ORDER BY 3,1
        ;""".format(weekly_update[0],weekly_update[1]))
        df_subinfo = get_data_from_snowflake("""SELECT application_status,
        detailed_application_status,
        application_status_category,
        SUM_SALARY_INCOME_TXN_0_365,
        TOTAL_BALANCE,
        NEG_BAL_OCCURENCES_30D,
        cast(profile_creation_ts as date),
        COUNT(*)
        FROM credit.application.application_summary
        WHERE testing_stage = 'Rollout'
            AND application_recency = 1
            AND (cast(profile_creation_ts as date) between {} and {})
            AND (DETAILED_APPLICATION_STATUS = 'Income reported is insufficient' or lower(DETAILED_APPLICATION_STATUS) = 'number of recent negative balance occurrences is too high' or lower(DETAILED_APPLICATION_STATUS) = 'total connected account balance is too low')
        GROUP BY 1,2,3,4,5,6,7
        ORDER BY 3,1
        ;""".format(weekly_update[0],weekly_update[1]))

    # rename column count(*) to count
    df_onboarding = df_onboarding.rename(columns={'COUNT(*)': 'count'})
    print(df_onboarding)

    # create a new column called percentage of total
    df_onboarding['percentage_of_total'] = (df_onboarding['count'] / df_onboarding['count'].sum()) * 100
    print(df_onboarding)
    print("Null values in the DETAILED_APPLICATION_STATUS: {}".format(df_onboarding['DETAILED_APPLICATION_STATUS'].isnull().values.any().sum()))
    print("Null values in the APPLICATION_STATUS_CATEGORY: {}".format(df_onboarding['APPLICATION_STATUS_CATEGORY'].isnull().values.any().sum()))

    df_onboarding.loc[df_onboarding['DETAILED_APPLICATION_STATUS'].str.lower() == 'pending docv', 'DETAILED_APPLICATION_STATUS'] = 'DocV Drop Offs'
    df_onboarding.loc[df_onboarding['DETAILED_APPLICATION_STATUS'].str.lower() == 'valid residential address not submitted', 'DETAILED_APPLICATION_STATUS'] = 'Invalid Address '
    df_onboarding.loc[df_onboarding['DETAILED_APPLICATION_STATUS'].str.lower() == 'number of accounts 120 days or more delinquent is too high', 'DETAILED_APPLICATION_STATUS'] = 'Too Many Recent Charge-offs'
    df_onboarding.loc[df_onboarding['DETAILED_APPLICATION_STATUS'].str.lower() == 'total utilization on revolving accounts is too high', 'DETAILED_APPLICATION_STATUS'] = 'Bureau Utilization Too High'
    df_onboarding.loc[df_onboarding['DETAILED_APPLICATION_STATUS'].str.lower() == 'total connected account balance is too low', 'DETAILED_APPLICATION_STATUS'] = 'Balance <= $25'
    df_onboarding.loc[df_onboarding['DETAILED_APPLICATION_STATUS'].str.lower() == 'number of recent negative balance occurrences is too high', 'DETAILED_APPLICATION_STATUS'] = 'Too Many Neg Bal Occs'
    df_onboarding.loc[df_onboarding['DETAILED_APPLICATION_STATUS'].str.lower() == 'multiple income', 'DETAILED_APPLICATION_STATUS'] = 'Multiple Failures'

    df_onboarding.loc[df_onboarding['DETAILED_APPLICATION_STATUS'].str.lower() == 'active occurrence of severe derogatory event', 'DETAILED_APPLICATION_STATUS'] = 'Other Failures'
    df_onboarding.loc[df_onboarding['DETAILED_APPLICATION_STATUS'].str.lower() == 'fraud tag on credit report', 'DETAILED_APPLICATION_STATUS'] = 'Other Failures'
    df_onboarding.loc[df_onboarding['DETAILED_APPLICATION_STATUS'].str.lower() == 'equifax error', 'DETAILED_APPLICATION_STATUS'] = 'Other'
    df_onboarding.loc[df_onboarding['DETAILED_APPLICATION_STATUS'].str.lower() == 'execution error', 'DETAILED_APPLICATION_STATUS'] = 'Other'
    df_onboarding.loc[df_onboarding['DETAILED_APPLICATION_STATUS'].str.lower() == 'pending cha', 'DETAILED_APPLICATION_STATUS'] = 'Other'
    df_onboarding.loc[df_onboarding['DETAILED_APPLICATION_STATUS'].isnull(), 'DETAILED_APPLICATION_STATUS'] = 'Other'

    df_onboarding.loc[df_onboarding['APPLICATION_STATUS_CATEGORY'] == 'Manual Reject', 'APPLICATION_STATUS_CATEGORY'] = 'Model'
    df_onboarding.loc[df_onboarding['APPLICATION_STATUS_CATEGORY'] == 'Approval', 'APPLICATION_STATUS_CATEGORY'] = 'Approvals'
    df_onboarding.loc[df_onboarding['APPLICATION_STATUS_CATEGORY'] == 'Credit Decline', 'APPLICATION_STATUS_CATEGORY'] = 'Credit Failures'
    df_onboarding.loc[df_onboarding['APPLICATION_STATUS_CATEGORY'] == 'KYC Decline', 'APPLICATION_STATUS_CATEGORY'] = 'KYC Rejects'
    df_onboarding.loc[df_onboarding['APPLICATION_STATUS_CATEGORY'] == 'Income Decline', 'APPLICATION_STATUS_CATEGORY'] = 'Income Declines'

    df_onboarding.loc[df_onboarding['DETAILED_APPLICATION_STATUS'] == 'Pre-KYC Drop Off', 'APPLICATION_STATUS_CATEGORY'] = 'Unfinished'
    df_onboarding.loc[df_onboarding['DETAILED_APPLICATION_STATUS'] == 'Manual Review', 'APPLICATION_STATUS_CATEGORY'] = 'Unfinished'
    df_onboarding.loc[df_onboarding['APPLICATION_STATUS_CATEGORY'].isnull(), 'APPLICATION_STATUS_CATEGORY'] = 'Unfinished'
    df_onboarding.loc[df_onboarding['DETAILED_APPLICATION_STATUS'] == 'Percent of accounts in good standing in the past 6 months is too low', 'APPLICATION_STATUS_CATEGORY'] = 'Credit Failures'

    # group by APPLICATION_STATUS_CATEGORY and then by DETAILED_APPLICATION_STATUS and then sum the count
    df_sublevels = df_onboarding.groupby(['APPLICATION_STATUS_CATEGORY', 'DETAILED_APPLICATION_STATUS']).sum()
    print(df_sublevels)

    total_group = df_onboarding.groupby(['APPLICATION_STATUS_CATEGORY']).sum()
    print(total_group)

    # create a script that will generate the daily/weekly report
    if daily_update:
        onboarding_date = pd.to_datetime(daily_update).strftime('%B %d, %Y')
    else:
        onboarding_date = "{} to {}".format(pd.to_datetime(weekly_update[0]).strftime('%B %d, %Y'),pd.to_datetime(weekly_update[1]).strftime('%B %d, %Y'))
    # create a file and write the report to it
    with open('onboarding_funnel_report.md', 'w') as f:
        f.write("\n**{}** Onboarding Funnel Summary ({} Profiles Created)".format(onboarding_date,df_onboarding['count'].sum()))
        list_of_kpis = total_group.index.get_level_values('APPLICATION_STATUS_CATEGORY')
        f.write("\n\u2022 {} {} ({}%)".format(total_group['count'][0],list_of_kpis[0],round(total_group['percentage_of_total'][0])))
        for kpi,count,perc in zip(list_of_kpis[1:], total_group['count'][1:], total_group['percentage_of_total'][1:]):
            f.write("\n\u2022 {} {} ({}%)".format(count,kpi,round(perc)))
            subcounts = df_sublevels.loc[kpi].get('count')
            for i in zip(subcounts.index, subcounts):
                f.write("\n\t\u25CB {} {}".format(i[1],i[0]))
        f.close()

    # for getting bucketed data
    df_subinfo = df_subinfo.rename(columns={'COUNT(*)': 'count'})
    # lower is exclusive, upper is inclusive
    # 750 value would get put into 500-750 bucket
    # 999 value would get put into 750-999 bucket
    # print("Null values in the NEG_BAL_OCCURENCES_30D: {}".format(df_subinfo['NEG_BAL_OCCURENCES_30D'].isna().sum()))
    # print("Null values in the AVG_INFLOW_6M: {}".format(df_subinfo['AVG_INFLOW_6M'].isna().sum()))
    df_subinfo.dropna(subset=['NEG_BAL_OCCURENCES_30D'], inplace=True)
    df_subinfo.dropna(subset=['SUM_SALARY_INCOME_TXN_0_365'], inplace=True)
    df_subinfo.dropna(subset=['TOTAL_BALANCE'], inplace=True)
    df_subinfo['NEG_BAL_OCCURENCES_30D'] = df_subinfo['NEG_BAL_OCCURENCES_30D'].astype(int)
    df_subinfo['income_bucket'] = pd.cut(df_subinfo['SUM_SALARY_INCOME_TXN_0_365'], bins=[0, 5000, 10000, 15000, 20000], labels=['$0-$5K', '$5K-$10K', '$10K-$15K', '$15K-$20K'])
    df_subinfo['neg_occurences'] = pd.cut(df_subinfo['NEG_BAL_OCCURENCES_30D'], bins=[8,10,11,15,16,20], labels=['8-10','10-11','11-15','15-16','16-20'],include_lowest=True)
    df_subinfo['total_balances'] = pd.cut(df_subinfo['TOTAL_BALANCE'], bins=[-50,0,0.01,25], labels=['-$50-$0','$0-$0.01','$0.01-$25'],include_lowest=True)

    income_buckets = df_subinfo[df_subinfo["DETAILED_APPLICATION_STATUS"] == "Income reported is insufficient"]
    neg_occ_buckets = df_subinfo[df_subinfo["DETAILED_APPLICATION_STATUS"] == "Number of recent negative balance occurrences is too high"]
    total_balance_buckets = df_subinfo[df_subinfo["DETAILED_APPLICATION_STATUS"] == "Total connected account balance is too low"]

    income_buckets.loc[income_buckets['SUM_SALARY_INCOME_TXN_0_365'] == 0, 'income_bucket'] = '$0-$5K'
    income_buckets['income_bucket'] = income_buckets['income_bucket'].cat.add_categories('$20k+')
    income_buckets.loc[income_buckets['SUM_SALARY_INCOME_TXN_0_365'] > 20000, 'income_bucket'] = '$20k+'

    neg_occ_buckets['neg_occurences'] = neg_occ_buckets['neg_occurences'].cat.add_categories(['<8'])
    neg_occ_buckets.loc[neg_occ_buckets['NEG_BAL_OCCURENCES_30D'] < 8, 'neg_occurences'] = '<8'
    neg_occ_buckets['neg_occurences'] = neg_occ_buckets['neg_occurences'].cat.add_categories(['21+'])
    neg_occ_buckets.loc[neg_occ_buckets['NEG_BAL_OCCURENCES_30D'] >= 21, 'neg_occurences'] = '21+'

    total_balance_buckets['total_balances'] = total_balance_buckets['total_balances'].cat.add_categories('< -$50')
    total_balance_buckets.loc[total_balance_buckets['TOTAL_BALANCE'] < -50, 'total_balances'] = '< -$50'
    total_balance_buckets['total_balances'] = total_balance_buckets['total_balances'].cat.add_categories('$25+')
    total_balance_buckets.loc[total_balance_buckets['TOTAL_BALANCE'] > 25, 'total_balances'] = '$25+'


    # make a percentage column for each bucket 
    income_buckets['percentage'] = income_buckets['count']/income_buckets['count'].sum() * 100
    neg_occ_buckets['percentage'] = neg_occ_buckets['count']/neg_occ_buckets['count'].sum() * 100
    total_balance_buckets['percentage'] = total_balance_buckets['count']/total_balance_buckets['count'].sum() * 100
    final_income_buckets = income_buckets.groupby(['income_bucket']).agg({'percentage': 'sum'})
    final_neg_buckets = neg_occ_buckets.groupby(['neg_occurences']).agg({'percentage': 'sum'})
    final_total_balance = total_balance_buckets.groupby(['total_balances']).agg({'percentage': 'sum'})

    # print(df_subinfo)
    # print(income_buckets[['USER_ID','DETAILED_APPLICATION_STATUS','SUM_SALARY_INCOME_TXN_0_365','income_bucket']])
    print(neg_occ_buckets[['DETAILED_APPLICATION_STATUS','NEG_BAL_OCCURENCES_30D','neg_occurences']])
    # remove categories with 0% count
    final_income_buckets = final_income_buckets[final_income_buckets['percentage'] != 0]
    final_neg_buckets = final_neg_buckets[final_neg_buckets['percentage'] != 0]
    final_total_balance = final_total_balance[final_total_balance['percentage'] != 0]

    with open('onboarding_funnel_report.md', 'a') as f:
        f.write("\nFunnel Breakdown")
        f.write("\n\u2022 Too Many Neg Bal Occs")
        for i,j in zip(final_neg_buckets.index.values,final_neg_buckets['percentage']):
            f.write("\n\t\u25CB {} : {}%".format(i,round(j)))
        f.write("\n\u2022 Income reported is insufficient")
        for i,j in zip(final_income_buckets.index.values,final_income_buckets['percentage']):
            f.write("\n\t\u25CB {} : {}%".format(i,round(j)))
        f.write("\n\u2022 Balance <= $25")
        for i,j in zip(final_total_balance.index.values,final_total_balance['percentage']):
            f.write("\n\t\u25CB {} : {}%".format(i,round(j)))
        f.close()

   

yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')

# Uncomment this function for daily update
# get_results(daily_update=f"'{yesterday}'")

# Uncomment this section to run the function for the weekend + friday update

# friday = (date.today() - timedelta(days=3)).strftime('%Y-%m-%d')
# get_results(weekly_update=[f"'{friday}'",f"'{yesterday}'"])
