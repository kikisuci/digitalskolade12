from src.snowflake import Snowflake
from datetime import datetime, timedelta, date


SF_OBJ = None


def get_date_filter():
    # Get today's date
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)

    # Get the first date of the current month
    startDate = date(yesterday.year, yesterday.month, 1)
    return startDate.strftime("%Y-%m-%d")


def check_if_start_month():
    today = date.today()
    start_date = date(today.year, today.month, 1) 
    return today == start_date


def generate_data_mart():
    table_name = 'monthly_category_sales'

    # Create the table if it doesn't exist
    if not SF_OBJ.check_table_exists(table_name):
        create_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                ORDER_DATE DATE,
                CATEGORY_ID NUMBER(38,0),
                TOTAL_SALES FLOAT
            )
        """
        SF_OBJ.create_table(create_query)

    startDate = get_date_filter()

    read_query = f"""
       SELECT TO_DATE(TO_CHAR(ORDER_DATE, 'YYYY-MM-01'), 'YYYY-MM-DD') AS "ORDER_DATE",
            C.CATEGORY_ID,
            SUM(OD.QUANTITY*OD.UNIT_PRICE) "TOTAL_SALES"
        FROM FINAL_PROJECT_WH.PUBLIC.ORDERS O
            INNER JOIN FINAL_PROJECT_WH.PUBLIC.ORDER_DETAILS OD ON OD.ORDER_ID = O.ORDER_ID
            INNER JOIN FINAL_PROJECT_WH.PUBLIC.PRODUCTS P ON P.PRODUCT_ID = OD.PRODUCT_ID
            INNER JOIN FINAL_PROJECT_WH.PUBLIC.CATEGORIES C ON C.CATEGORY_ID = P.CATEGORY_ID
        WHERE ORDER_DATE > '{startDate}'
            AND ORDER_DATE < DATEADD(MONTH, 1, '{startDate}')
        GROUP BY TO_DATE(TO_CHAR(ORDER_DATE, 'YYYY-MM-01'), 'YYYY-MM-DD'),
            C.CATEGORY_ID
        ORDER BY "CATEGORY_ID"
    """

    SF_OBJ.fill_data_mart(read_query, table_name, "ORDER_DATE, CATEGORY_ID, TOTAL_SALES")


if __name__ == '__main__':
    SF_OBJ = Snowflake()
    if check_if_start_month():
        generate_data_mart()
