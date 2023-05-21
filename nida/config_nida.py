import snowflake.connector

con_def = snowflake.connector.connect(user='nidaalyssads',
                                     host='zh80053.ap-southeast-3.aws.snowflakecomputing.com',
                                     account='zh80053',
                                     region = 'ap-southeast-3',
                                     password ='simaS123',
                                     database='NORTHWIND',      
                                     warehouse='COMPUTE_WH',  
                                     schema ='PUBLIC',
                                     autocommit=True)         

db_cursor_def = con_def.cursor()

db_cursor_def.execute("""
                UPDATE CONFIG
                SET REPORT_DATE = CURRENT_DATE
                ;"""
                     )