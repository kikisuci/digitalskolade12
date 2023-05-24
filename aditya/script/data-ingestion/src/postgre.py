import psycopg2
from dotenv import dotenv_values

class  Postgre():
    def __init__(self):
        env_values = dotenv_values()

        POSTGRE_HOST = env_values['POSTGRE_HOST']
        POSTGRE_PORT = env_values['POSTGRE_PORT'] 
        POSTGRE_DATABASE = env_values['POSTGRE_DATABASE']
        POSTGRE_USER = env_values['POSTGRE_USER']
        POSTGRE_PASSWORD = env_values['POSTGRE_PASSWORD']

        self.conn = psycopg2.connect(
            host=POSTGRE_HOST,
            port=POSTGRE_PORT,
            database=POSTGRE_DATABASE,
            user=POSTGRE_USER,
            password=POSTGRE_PASSWORD
        )

        self.sf_obj = None

    def get_data(self, query):
        cur = self.conn.cursor()
        cur.execute(query)

        rows = cur.fetchall()
        
        columns = [desc[0] for desc in cur.description]
        cur.close()

        return rows, columns

    def column_desc(self, table_name):
        # Retrieve the table schema
        table_column, _ = self.get_data(f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}'
                AND data_type <> 'bytea'

        """)
        return table_column
        

    def set_sf_conn(self, sf_conn):
        self.sf_conn = sf_conn
        
