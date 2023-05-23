from src.postgre import Postgre
from src.snowflake import Snowflake
from datetime import datetime, timedelta


PG_OBJ = None
SF_OBJ = None


def get_date_filter():
    # Define yesterday date for date filter
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")


def create_table_sf_query(table_name, table_column):
    query = f"CREATE TABLE IF NOT EXISTS {table_name} ("

    for column in table_column:
        column_name = column[0]
        data_type = column[1]
        query += f"{column_name} {data_type}, "

    query = query[:-2]
    query += ")"

    return query


def fact_ingestion(table_name, query_population):
    data_rows, data_column = PG_OBJ.get_data(query_population)
    
    # Create the table if it doesn't exist
    if not SF_OBJ.check_table_exists(table_name):
        table_column = PG_OBJ.column_desc(table_name)
        SF_OBJ.create_table_snowflake(
            create_table_sf_query(table_name, table_column))

    if len(data_rows)<=0: 
        return
    
    SF_OBJ.post_fact_table(table_name, data_rows, data_column)


def dim_ingestion(table_name):
    table_column = PG_OBJ.column_desc(table_name)
    cols = ''
    for column in table_column:
        cols += f"{column[0]}, "

    query_population = f"SELECT {cols[:-2]} FROM {table_name}"
    data_rows, data_column = PG_OBJ.get_data(query_population)
    
    # Create the table if it doesn't exist
    if not SF_OBJ.check_table_exists(table_name):
        SF_OBJ.create_table_snowflake(
            create_table_sf_query(table_name, table_column))

    if len(data_rows)<=0: 
        return
    
    SF_OBJ.post_fact_table(table_name, data_rows, data_column)

def ingest_orders():
    table_name = 'orders'
    query = f"""
            SELECT * 
            FROM {table_name}
            WHERE order_date = '{get_date_filter()}'
        """
    fact_ingestion(table_name, query)


def ingest_order_details():
    table_name = 'order_details'
    query = f"""
            SELECT OD.* 
            FROM orders O
                INNER JOIN {table_name} OD on O.order_id = OD.order_id
            WHERE O.order_date = '{get_date_filter()}'
        """
    fact_ingestion(table_name, query)


def ingest_dim_table():
    table_list = ['categories', 'customer_customer_demo', 'customer_demographics', 'customers',
                  'employee_territories', 'employees', 'products', 'region', 'shippers', 
                  'suppliers', 'territories', 'us_states']
    
    for table_name in table_list:
        dim_ingestion(table_name)

if __name__ == "__main__":
    # create class object
    PG_OBJ = Postgre()
    SF_OBJ = Snowflake()

    PG_OBJ.set_sf_conn(SF_OBJ)
    SF_OBJ.set_pg_conn(PG_OBJ)

    # data ingestion start 
    ingest_orders()
    ingest_order_details()
    ingest_dim_table()

    # close connection
    PG_OBJ.conn.close()
    SF_OBJ.conn.close()
    



