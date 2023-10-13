# Snowpark for Python
from snowflake.snowpark.session import Session
from snowflake.snowpark.version import VERSION
# Misc
import json
import logging 
logger = logging.getLogger("snowflake.snowpark.session")
logger.setLevel(logging.ERROR)
from scipy import stats
import statsmodels.api as sm
from statsmodels.discrete.discrete_model import Probit
import scipy.stats as ss
import numpy as np
import pandas as pd
# Create Snowflake Session object
connection_parameters = json.load(open('connection.json'))
session = Session.builder.configs(connection_parameters).create()
session.sql_simplifier_enabled = True

snowflake_environment = session.sql('select current_user(), current_version()').collect()
snowpark_version = VERSION
# list of features
features = ['NUM_INQ_3M', 'NUM_INQ_24M', 'PCT_UTIL_INQ_12M_TO_INQ_24M',
       'NUM_UTIL_INQ_6M', 'NUM_NON_UTIL_INQ_6M', 'AGE_OLDEST_BANKCARD_TRADE',
       'AGE_NEWEST_TRADE', 'AGE_NEWEST_BANKCARD_TRADE',
       'AGE_NEWEST_DEP_STORE_TRADE', 'NUM_OPEN_CREDIT_UNION_TRADES',
       'NUM_OPEN_MORTGAGE_TRADES', 'TOT_BAL_OPEN_CREDIT_UNION_TRADES',
       'TOT_BAL_OPEN_DEP_STORE_TRADES', 'TOT_BAL_OPEN_INSTALLMENT_TRADES',
       'TOT_BAL_OPEN_RETAIL_TRADES', 'NUM_MORTGAGE_TRADES',
       'TOT_HIGH_CREDIT_OPEN_BANKCARD', 'TOT_LOAN_AMT_OPEN_INSTALLMENT',
       'TOT_HIGH_CREDIT_OPEN_REVOLVING', 'NUM_TRADES_PAST_DUE_BALANCE',
       'NUM_REVOLVING_TRADES_PAST_DUE_BAL', 'TOT_PAST_DUE_BAL',
       'TOT_PAST_DUE_BAL_RETAIL', 'NUM_30D_PAST_DUE_24M',
       'NUM_60PLUSD_PAST_DUE_24M', 'NUM_60PLUSD_PAST_DUE_24M_REVOLVING',
       'NUM_TRADES_WORST_6M_30D_PAST_DUE', 'NUM_TRADES_30PLUSD_PAST_DUE',
       'NUM_OPEN_BANKCARD_TRADES_UTIL_GTE_75PCT', 'NUM_UNPAID_COLLECTIONS',
       'TOTAL_UNPAID_COLLECTIONS_BAL_12M', 'TOTAL_UNPAID_COLLECTIONS_BAL_24M',
       'DISCHARGED_BANKRUPTY', 'PCT_BANKCARD_TO_TOT_TRADES',
       'PCT_REVOLVING_TO_TOT_TRADES', 'PCT_TRADES_OPENED_12M_TO_TOT_TRADES',
       'TOT_BAL_REVOLVING_TRADES', 'TOT_UTIL_BANKCARD_TRADES',
       'PCT_BAL_PAST_DUE_ALL_TRADES',
       'PCT_GOOD_REVOLVING_TRADES_TO_TOT_REVOLVING_TRADES',
       'PCT_TRADES_GOOD_6M_TO_TOT_TRADES_6M',
       'PCT_REVOLVING_TRADES_GOOD_6M_TO_REVOLVING_TRADES_6M',
       'NUM_TRADES_WORST_EVER_60PLUSD_PAST_DUE',
       'PCT_TRADES_WORST_EVER_60PLUSD_PAST_DUE_TO_TOT_TRADES',
       'AVG_SALARY_INCOME_TXN', 'AVG_UTILITIES_EXPENSE_TXN',
       'COUNT_ASSET_DEPOSIT_TXN', 'COUNT_NEG_BAL_OCC_0_180',
       'COUNT_NEG_BAL_OCC_0_30', 'COUNT_RENT_EXPENSE_TXN_0_365',
       'COUNT_TELECOM_EXPENSE_TXN_0_30', 'SM_CASH_OUT_DEBT_SER',
       'SUM_CASH_OUTFLOW_TRANSFERS', 'SUM_INSURANCE_EXPENSE_TXN',
       'SUM_SALARY_INCOME_TXN_0_365', 'SUM_UTILITIES_EXPENSE_TXN',
       'TOTAL_BALANCE']

def get_test_data():
    arro_test = session.sql("""select * from credit.application.arro_risk_model_1_summary
                      where user_id in (select user_id from CREDIT.APPLICATION.APPLICATION_SUMMARY 
                      where testing_stage = 'Rollout' AND application_recency = 1 AND lower(APPLICATION_STATUS) = 'approved')  
                       and APPLICATION_START_TS > '2023-10-04'
    """)
    # limit 3862
    arro_test = arro_test[features]
    arro_test = arro_test.to_pandas()
    arro_data = pd.read_csv('data/arro_data.csv')

    # get the file with the null mapping
    null_mapping = pd.read_csv('data/new_null_mapping.csv',index_col=0)
    for index, row in null_mapping.iterrows():
        null_mapping.at[index, 'NULL Values'] = [float(x) for x in row['NULL Values'].split(',')]

    # data pre-processing
    for i in arro_test.columns:
        val = null_mapping.loc[i]['NULL Values']
        # arro_data[i] = arro_data[i].apply(lambda x: np.nan if x in val else x) -> already been pre-processed
        arro_test[i] = arro_test[i].apply(lambda x: np.nan if x in val else x)
    for i in arro_test.columns:
        # if (arro_data[i].isnull().sum() > 0.6 * len(arro_data)) or (arro_test[i].isnull().sum() > 0.6 * len(arro_test)):
        if (arro_test[i].isnull().sum() > 0.6 * len(arro_test)):
            try:
                arro_data.drop(i, axis=1, inplace=True)
            except:
                pass
            arro_test.drop(i, axis=1, inplace=True)
    for i in arro_test.columns:
        arro_test[i].fillna(arro_test[i].median(), inplace=True)
    try:
        arro_test.drop('PCT_UTIL_INQ_12M_TO_INQ_24M', axis=1, inplace=True)
    except:
        pass
    return arro_test[arro_data.columns]
