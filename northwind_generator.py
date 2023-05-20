import psycopg2
import random

DB_NAME = "defaultdb"
DB_USER = "doadmin"
DB_PASS = "AVNS_Lx9PbZB62Y5BMnMI6UA"
DB_HOST = "db-postgresql-nyc1-80919-do-user-8304997-0.b.db.ondigitalocean.com"
DB_PORT = "25060"
 

conn = psycopg2.connect(database=DB_NAME,
                        user=DB_USER,
                        password=DB_PASS,
                        host=DB_HOST,
                        port=DB_PORT)
cur = conn.cursor()  # creating a cursor


rand = random.randint(5, 20)
for x in range(rand):
    # executing queries to create table
    cur.execute("""
    insert into orders
    with max_id as (select max(order_id)+1 as order_id from orders )
    select max_id.order_id,customer_id ,employee_id ,current_date as order_date, current_date + interval '1 months' as required_date ,
    current_date + interval '10 days' as shipped_date , ship_via,freight ,ship_name,ship_address ,ship_city,ship_region , ship_postal_code ,ship_country 
    FROM orders,max_id
    ORDER BY random()
    LIMIT 1;
    """)
    # commit the changes
    conn.commit()

    cur.execute("""
    insert into order_details
    with max_id as (select max(order_id) as order_id from orders )
    SELECT distinct max_id.order_id,product_id,unit_price,quantity,discount from order_details od,max_id 
    limit 1;
    """)
    # commit the changes
    conn.commit()

    rand_2 = random.randint(3, 8)
    for y in range(rand_2):
        cur.execute("""
        insert into order_details
        with max_id as (select max(order_id) as order_id from orders ),
        get_order_details as (select distinct od.order_id,product_id from order_details od join max_id m on od.order_id = m.order_id where od.order_id = m.order_id)
        SELECT max_id.order_id,product_id,unit_price,quantity,discount from order_details od,max_id 
        where product_id not in (select distinct product_id from get_order_details)
        ORDER BY random()
        limit 1;
        """)
        # commit the changes
        conn.commit()
conn.close()
print("Table Created successfully")
