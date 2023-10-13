import snowflake.connector
from config import *

def get_data_from_snowflake(query):
    ctx = snowflake.connector.connect(
        user=user,
        password=password,
        account=account
    )
    cs = ctx.cursor()
    try:
        cs.execute(query)
        df = cs.fetch_pandas_all()
    finally:
        cs.close()
    ctx.close()
    return df
