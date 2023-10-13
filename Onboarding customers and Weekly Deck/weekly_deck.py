from get_data import get_data_from_snowflake
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import matplotlib.ticker as ticker
import matplotlib.dates as mdates


def get_results(daily_update=None,weekly_update=None):

    if weekly_update:
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
    # rename column count(*) to count
    print(df_onboarding)
    df_onboarding = df_onboarding.rename(columns={'COUNT(*)': 'count'})
    # create a new column called percentage of total
    df_onboarding['percentage_of_total'] = (df_onboarding['count'] / df_onboarding['count'].sum()) * 100
    # print(df_onboarding)
    # if the df_onboarding['APPLICATION_STATUS_CATEGORY'] is "Unfinished" then find "error" in df_onboarding["DETAILED_APPLICATION_STATUS"]
    errors = df_onboarding[(df_onboarding['APPLICATION_STATUS_CATEGORY']=="Unfinished") & ((df_onboarding['DETAILED_APPLICATION_STATUS']=="Execution Error") | (df_onboarding['DETAILED_APPLICATION_STATUS']=="Equifax Error"))]
    errors = errors.groupby(['DETAILED_APPLICATION_STATUS']).agg({'count': 'sum'}).reset_index()
    print("\n")
    print("Application failures",errors['count'].sum())
    print(errors)
    print("\n")

     
    # print("Null values in the DETAILED_APPLICATION_STATUS: {}".format(df_onboarding['DETAILED_APPLICATION_STATUS'].isnull().values.any().sum()))
    # print("Null values in the APPLICATION_STATUS_CATEGORY: {}".format(df_onboarding['APPLICATION_STATUS_CATEGORY'].isnull().values.any().sum()))

    df_onboarding.loc[df_onboarding['DETAILED_APPLICATION_STATUS'].str.lower() == 'pending docv', 'DETAILED_APPLICATION_STATUS'] = 'DocV Drop Offs'
    df_onboarding.loc[df_onboarding['DETAILED_APPLICATION_STATUS'].str.lower() == 'valid residential address not submitted', 'DETAILED_APPLICATION_STATUS'] = 'Invalid Address '
    df_onboarding.loc[df_onboarding['DETAILED_APPLICATION_STATUS'].str.lower() == 'number of accounts 120 days or more delinquent is too high', 'DETAILED_APPLICATION_STATUS'] = 'Too Many Recent Charge-offs'
    df_onboarding.loc[df_onboarding['DETAILED_APPLICATION_STATUS'].str.lower() == 'total utilization on revolving accounts is too high', 'DETAILED_APPLICATION_STATUS'] = 'Bureau Utilization Too High'
    df_onboarding.loc[df_onboarding['DETAILED_APPLICATION_STATUS'].str.lower() == 'total connected account balance is too low', 'DETAILED_APPLICATION_STATUS'] = 'Balance <= -$50'
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
    # if the df_onboarding['DETAILED_APPLICATION_STATUS'] is anything other than 'Pre-KYC Drop Off' or 'Pending Plaid' mark it as Other
    df_onboarding.loc[~df_onboarding['DETAILED_APPLICATION_STATUS'].isin(['Pre-KYC Drop Off', 'Pending Plaid']), 'DETAILED_APPLICATION_STATUS'] = 'Other'

    # group by APPLICATION_STATUS_CATEGORY and then by DETAILED_APPLICATION_STATUS and then sum the count
    df_sublevels = df_onboarding.groupby(['APPLICATION_STATUS_CATEGORY', 'DETAILED_APPLICATION_STATUS']).sum()
    # print(df_sublevels)
    total_group = df_onboarding.groupby(['APPLICATION_STATUS_CATEGORY']).sum()
    list_of_kpis = total_group.index.get_level_values('APPLICATION_STATUS_CATEGORY')
    # print(total_group)
    print("\n")
    # create a script that will generate the weekly report
    for i in list_of_kpis:
        if i.startswith('Approvals'):
            print("New customers last 7 days : ", total_group.loc[i]['count'])
            print("Conversion last 7 days : {}%".format(round(total_group.loc[i]['percentage_of_total'])))
            break
    
    print("Total active customers : ", get_data_from_snowflake("select COUNT(DISTINCT user_id) as active_customers FROM credit.customer.user_profile WHERE lower(activity_status) in ('current','pending','active-late','active-delinquent')")["ACTIVE_CUSTOMERS"][0])
    print("Accounts created in the last 7 days : ", df_onboarding['count'].sum())

    print("\n")
    declined = df_onboarding['count'].sum()-(total_group.loc['Approvals']['count']+total_group.loc['Unfinished']['count'])
    print("Total application declined last 7 days : {}({}%)".format(declined,round(declined/df_onboarding['count'].sum()*100)))
    onboarding_date = "{} to {}".format(pd.to_datetime(weekly_update[0]).strftime('%B %d, %Y'),pd.to_datetime(weekly_update[1]).strftime('%B %d, %Y'))
    for i in list_of_kpis[1:]:
        print('{} : {}({}%)'.format(i,total_group.loc[i]['count'],round(total_group.loc[i]['percentage_of_total'])))
    
    print("\n")
    for kpi in list_of_kpis:
        if kpi.startswith('Unfinished'):
            subcounts = df_sublevels.loc[kpi].get('count')
            subperc = df_sublevels.loc[kpi].get('percentage_of_total')
            # print(subperc)
            for i in zip(subcounts.index, subcounts,subperc):
                print("{} {}({}%)".format(i[0],i[1],round(i[2])))
    
df_graph = get_data_from_snowflake("""SELECT
    (TO_CHAR(DATE_TRUNC('week', APPLICATION_START_TS ), 'YYYY-MM-DD')) as application_start_ts_date,
    COUNT(DISTINCT CASE WHEN (APPLICATION_STATUS) = 'approved' THEN (USER_ID) END) / NULLIF(COUNT(DISTINCT CASE WHEN (APPLICATION_STATUS) IN ('declined-identity','failed','approved') AND (APPLICATION_RECENCY) = 1 THEN (USER_ID)
    WHEN (INCOME_REJECT_REASON) IS NOT NULL AND (APPLICATION_RECENCY) = 1 THEN (USER_ID) END ), 0) AS approval_rate,
    COUNT(DISTINCT CASE WHEN (APPLICATION_STATUS) = 'approved' THEN (USER_ID) END) AS approvals
    FROM credit.application.APPLICATION_SUMMARY
    WHERE (APPLICATION_RECENCY) = 1 AND ((( PROFILE_CREATION_TS) >= ((DATEADD('year', 0, DATE_TRUNC('year', CURRENT_DATE())))) AND (PROFILE_CREATION_TS) < ((CURRENT_TIMESTAMP())))) AND (TESTING_STAGE) = 'Rollout'
    GROUP BY (DATE_TRUNC('week',APPLICATION_START_TS))
    ORDER BY 1""")

# Uncomment the below code to generate the weekly graph

# df_graph['APPROVAL_RATE'] = df_graph['APPROVAL_RATE'] * 100
# df_graph = df_graph[df_graph['APPLICATION_START_TS_DATE'] > '2023-05-22']
# df_graph['APPROVALS_CUMULATIVE'] = df_graph['APPROVALS'].cumsum()
# df_graph['APPLICATION_START_TS_DATE'] = pd.to_datetime(df_graph['APPLICATION_START_TS_DATE'])
# plt.figure(figsize=(10, 6))
# sns.lineplot(x="APPLICATION_START_TS_DATE", y="APPROVALS_CUMULATIVE", data=df_graph,color='g',label = 'Approvals (Cum.)')
# plt.ylabel('Approvals')
# plt.xlabel('')
# plt.legend(loc='best', bbox_to_anchor=(0.75, -0.1))
# ax2 = plt.twinx()
# sns.lineplot(x="APPLICATION_START_TS_DATE", y="APPROVAL_RATE", data=df_graph, ax=ax2,color='b',label = 'Approval Rate (Weekly)')
# ax2.set_ylim(bottom=0)
# ax2.set_xlim(left=df_graph['APPLICATION_START_TS_DATE'].min(), right=df_graph['APPLICATION_START_TS_DATE'].max())
# plt.ylabel('Approval Rate')
# ax2.yaxis.set_major_formatter(ticker.PercentFormatter(decimals=0))
# ax2.yaxis.set_major_locator(ticker.MultipleLocator(10))
# ax2.xaxis.set_major_locator(mdates.DayLocator(interval=14))
# ax2.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d/%Y"))
# plt.legend(loc='best', bbox_to_anchor=(0.45, -0.1))
# plt.savefig('weekly_approvals.png',bbox_inches='tight')

# Insert the dates here for the weekly report
get_results(weekly_update=["'2023-09-24'","'2023-09-30'"])










#