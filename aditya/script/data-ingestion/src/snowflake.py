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

        self.pg_conn = None

    def create_table_snowflake(self, query):
        cur = self.conn.cursor()
        query = query.replace("bytea", "binary")
        cur.execute(query)
        cur.close()

    def post_fact_table(self, table_name, pg_data, pg_column):
        cur = self.conn.cursor()

        # Ingest the data into Snowflake
        sf_data = [list(row) for row in pg_data]
        cur.executemany(f"INSERT INTO {table_name.upper()}({', '.join(pg_column)}) VALUES ({', '.join(['%s'] * len(sf_data[0]))})", sf_data)
        self.conn.commit()

        cur.close() 

    def post_dim_table(self, table_name, pg_data, pg_column):
        cur = self.conn.cursor()

        # Ingest the data into Snowflake
        for row in pg_data:
            print(list(row), row[0])
            cur.execute(f"""
                            INSERT INTO {table_name.upper()}({', '.join(pg_column)}) VALUES ({', '.join(['%s'] * len(sf_data[0]))})
                            WHERE NOT EXISTS ( 
                                SELECT * FROM {table_name.upper()} 
                                WHERE {pg_column[0]} = %s) 
                        """
                        , list(row), row[0])
            self.conn.commit()

        cur.close() 

    def check_table_exists(self, table_name):
        cur = self.conn.cursor()

        # Check if the table exists
        cur.execute(f"SHOW TABLES LIKE '{table_name.upper()}'")
        table_exists = cur.fetchone() is not None
        cur.close()

        return table_exists

    def set_pg_conn(self, pg_conn):
        self.pg_conn = pg_conn
