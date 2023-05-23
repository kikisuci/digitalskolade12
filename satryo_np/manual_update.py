from dags.scripts.SnowflakeTL import SnowflakeTL
import configparser

credentials = configparser.ConfigParser()
credentials.read('dags/credentials.ini')

task = SnowflakeTL(credentials)

with open('dags/scripts/daily_gross_revenue.sql', 'r') as fileSQL1:
    query = fileSQL1.read()
task.Send(query)

with open('dags/scripts/monthly_gross_revenue_product.sql', 'r') as fileSQL2:
    query = fileSQL2.read()
task.Send(query)

with open('dags/scripts/monthly_orders_product.sql', 'r') as fileSQL3:
    query = fileSQL3.read()
task.Send(query)

with open('dags/scripts/monthly_orders_catagories.sql', 'r') as fileSQL4:
    query = fileSQL4.read()
task.Send(query)

with open('dags/scripts/monthly_orders_country.sql', 'r') as fileSQL5:
    query = fileSQL5.read()
task.Send(query)
