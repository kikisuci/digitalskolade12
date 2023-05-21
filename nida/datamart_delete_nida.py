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
                DELETE FROM NIDA_TOT_GR_PER_DATE 
                WHERE ORDER_DATE = (SELECT REPORT_DATE FROM CONFIG);        
                """
                     )

db_cursor_def.execute("""                
                DELETE FROM NIDA_TOT_GR_PER_PRD_PER_MTH 
                WHERE ORDER_MONTH = (SELECT SUBSTR(REPORT_DATE,1,7) FROM CONFIG);  
                """
                     )

db_cursor_def.execute("""                
                DELETE FROM NIDA_TOT_OR_PER_PRD_PER_MTH 
                WHERE ORDER_MONTH = (SELECT SUBSTR(REPORT_DATE,1,7) FROM CONFIG);
                """
                     )

db_cursor_def.execute("""                
                DELETE FROM NIDA_TOT_OR_PER_CAT_PER_MTH 
                WHERE ORDER_MONTH = (SELECT SUBSTR(REPORT_DATE,1,7) FROM CONFIG);
                """
                     )

db_cursor_def.execute("""                
                DELETE FROM NIDA_TOT_OR_PER_CTRY_PER_MTH 
                WHERE ORDER_MONTH = (SELECT SUBSTR(REPORT_DATE,1,7) FROM CONFIG);
                """
                     )