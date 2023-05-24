from SnowflakeTL import SnowflakeTL
import configparser
import os


credentials = configparser.ConfigParser()
path = os.getcwd()
credentials.read(f'credentials.ini')

try:
    task = SnowflakeTL(credentials)
except KeyError:
    print("Error: 'snowflake' tidak ada di credentials")

with open('sql/daily_gross_revenue.sql', 'r') as fileSQL1:
    query = fileSQL1.read()
task.Send(query)

with open('sql/monthly_gross_revenue_product.sql', 'r') as fileSQL2:
    query = fileSQL2.read()
task.Send(query)

with open('sql/monthly_orders_product.sql', 'r') as fileSQL3:
    query = fileSQL3.read()
task.Send(query)

with open('sql/monthly_orders_catagories.sql', 'r') as fileSQL4:
    query = fileSQL4.read()
task.Send(query)

with open('sql/monthly_orders_country.sql', 'r') as fileSQL5:
    query = fileSQL5.read()
task.Send(query)
