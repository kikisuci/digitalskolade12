CREATE OR REPLACE TABLE DATAMART.MONTHLY_ORDERS_PRODUCT AS (
    WITH T1 as (
        SELECT DATE_ORDER.ORDER_DATE, DETAIL.PRODUCT_ID, DETAIL.QUANTITY
        FROM PUBLIC.ORDER_DETAILS AS DETAIL
        LEFT JOIN PUBLIC.ORDERS AS DATE_ORDER
        ON DETAIL.ORDER_ID = DATE_ORDER.ORDER_ID
        WHERE DATE_ORDER.ORDER_DATE >='2023-01-01'
    ),
    T2 AS (
        SELECT
            DATE_TRUNC('MONTH',T1.ORDER_DATE) AS MONTH,
            T1.PRODUCT_ID,
            SUM(T1.QUANTITY) AS SELL
        FROM T1
        GROUP BY 1, 2
    )
    SELECT T2.MONTH, PRODUCT.PRODUCT_NAME, T2.SELL
    FROM T2
    LEFT JOIN PUBLIC.PRODUCTS AS PRODUCT
    ON T2.PRODUCT_ID = PRODUCT.PRODUCT_ID
    ORDER BY T2.MONTH
);