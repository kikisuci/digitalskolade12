--GROSS REVENUE PERDAY
SELECT
  DATE(ord.ORDER_DATE) AS OrderDate,
  SUM(details.UNIT_PRICE * details.QUANTITY) AS GrossRevenue
FROM
  orders AS ord
JOIN order_details AS details 
ON ord.ORDER_ID = details.ORDER_ID
GROUP BY
  DATE(ord.ORDER_DATE)
ORDER BY
  DATE(ord.ORDER_DATE);
  
--Gross Revenue per product per bulan
SELECT
  MONTH(a.ORDER_DATE) AS OrderMonth,
  p.PRODUCT_NAME,
  SUM(b.UNIT_PRICE * b.QUANTITY) AS GrossRevenue
FROM
  orders AS a
  JOIN order_details AS b 
  ON a.ORDER_ID = b.ORDER_ID
  JOIN products AS p 
  ON b.PRODUCT_ID = p.PRODUCT_ID
GROUP BY
  MONTH(a.ORDER_DATE),
  p.PRODUCT_NAME
ORDER BY
  MONTH(a.ORDER_DATE),
  p.PRODUCT_NAME;

--Jumlah total pembelian per product per bulan
SELECT
  MONTH(a.ORDER_DATE) AS OrderMonth,
  p.PRODUCT_NAME,
  SUM(b.QUANTITY) AS Total_Order
FROM
  orders AS a
  JOIN order_details AS b ON a.ORDER_ID = b.ORDER_ID
  JOIN products AS p ON b.PRODUCT_ID = p.PRODUCT_ID
GROUP BY
  MONTH(a.ORDER_DATE),
  p.PRODUCT_NAME
ORDER BY
  MONTH(a.ORDER_DATE),
  p.PRODUCT_NAME;

--Jumlah total pembelian per kategori product per bulan
SELECT
  MONTH(a.ORDER_DATE) AS OrderMonth,
  c.CATEGORY_NAME,
  SUM(b.Quantity) AS TotalPembelian
FROM
  orders AS a
  JOIN order_details AS b ON a.ORDER_ID = b.ORDER_ID
  JOIN products AS p ON b.PRODUCT_ID = p.PRODUCT_ID
  JOIN categories AS c ON p.CATEGORY_ID = c.CATEGORY_ID
GROUP BY
  MONTH(a.ORDER_DATE),
  c.CATEGORY_NAME
ORDER BY
  MONTH(a.ORDER_DATE),
  c.CATEGORY_NAME;

--Jumlah total pembelian per negara per bulan
CREATE TABLE Fadly_Country_Dis
AS
SELECT
  MONTH(a.ORDER_DATE) AS OrderMonth,
  c.COUNTRY,
  SUM(b.QUANTITY) AS TotalPembelian
FROM
  orders AS a
  JOIN order_details AS b ON a.ORDER_ID = b.ORDER_ID
  JOIN customers AS c ON a.CUSTOMER_ID = c.CUSTOMER_ID
GROUP BY
  MONTH(a.ORDER_DATE),
  c.Country
ORDER BY
  MONTH(a.ORDER_DATE),
  c.Country;
