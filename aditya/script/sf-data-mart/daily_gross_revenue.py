from src.snowflake import Snowflake
from datetime import datetime, timedelta


SF_OBJ = None


def get_date_filter():
    # Define yesterday date for date filter
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")


def generate_data_mart():
    table_name = 'daily_gross_revenue'

    # Create the table if it doesn't exist
    if not SF_OBJ.check_table_exists(table_name):
        create_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                ORDER_DATE DATE,
                REVENUE FLOAT
            )
        """
        SF_OBJ.create_table(create_query)

    read_query = f"""
        SELECT  CAST(ORDER_DATE AS DATE) AS "ORDER_DATE",
            SUM((OD.QUANTITY*OD.UNIT_PRICE)-((OD.QUANTITY*OD.UNIT_PRICE)*DISCOUNT)) "REVENUE"
        FROM ORDERS O
            INNER JOIN ORDER_DETAILS OD ON OD.ORDER_ID=O.ORDER_ID
        WHERE ORDER_DATE = '{get_date_filter()}'
        GROUP BY CAST(ORDER_DATE AS DATE)
        ORDER BY "ORDER_DATE"
    """
    
    SF_OBJ.fill_data_mart(read_query, table_name, "ORDER_DATE, REVENUE")


if __name__ == '__main__':
    SF_OBJ = Snowflake()
    generate_data_mart()
