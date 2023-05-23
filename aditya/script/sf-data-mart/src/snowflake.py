import snowflake.connector
from dotenv import dotenv_values

class  Snowflake():
    def __init__(self):
        env_values = dotenv_values()
        SNOWFLAKE_USER = env_values['SNOWFLAKE_USER']
        SNOWFLAKE_PASSWORD = env_values['SNOWFLAKE_PASSWORD']
        SNOWFLAKE_ACCOUNT = env_values['SNOWFLAKE_ACCOUNT']
        SNOWFLAKE_WAREHOUSE = env_values['SNOWFLAKE_WAREHOUSE']
        SNOWFLAKE_DATABASE = env_values['SNOWFLAKE_DATABASE']
        SNOWFLAKE_SCHEMA = env_values['SNOWFLAKE_SCHEMA']

        self.conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )

    def create_table(self, query):
        cur = self.conn.cursor()
        query = query.replace("bytea", "binary")

        cur.execute(query)
        cur.close()


    def check_table_exists(self, table_name):
        cur = self.conn.cursor()

        # Check if the table exists
        cur.execute(f"SHOW TABLES LIKE '{table_name.upper()}'")
        table_exists = cur.fetchone() is not None
        cur.close()

        return table_exists

    def fill_data_mart(self, query_population, table_name, table_column):
        cur = self.conn.cursor()

        cur.execute(query_population)
        data_rows = cur.fetchall()

        if len(data_rows)<=0: 
            return
        
        cur.executemany(f"INSERT INTO {table_name}({table_column}) VALUES ({', '.join(['%s'] * len(data_rows[0]))})", data_rows)
        cur.close
