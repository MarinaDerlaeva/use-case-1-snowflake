
create DATABASE ECOMERSE_DB
USE DATABASE ECOMERSE_DB;
CREATE SCHEMA ECOMERSE_SCHEMA;


  CREATE OR REPLACE TABLE td_raw_orders (
  Order_ID STRING,
  Customer_ID STRING,
  Delivery_Address STRING,
  Payment_Method STRING,
  Quantity INT,
  Price FLOAT,
  Discount FLOAT,
  Final_Price FLOAT,
  Order_Status STRING,
  Order_Date STRING
);
COPY INTO td_raw_orders
FROM @my_stage/ecommerce_orders.csv
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_DELIMITER = ','  -- Разделителят е запетая
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'  -- Стойностите са обвити в двойни кавички
    SKIP_HEADER = 1  -- Пропускане на първия ред с заглавия
);


-- Таблици за прехвърляне на специфични записи
CREATE OR REPLACE TABLE td_for_review LIKE td_raw_orders;
CREATE OR REPLACE TABLE td_suspicious_records LIKE td_raw_orders;
CREATE OR REPLACE TABLE td_invalid_date_format LIKE td_raw_orders;
CREATE OR REPLACE TABLE td_negative_values LIKE td_raw_orders;
CREATE OR REPLACE TABLE td_clean_records LIKE td_raw_orders;

INSERT INTO td_for_review
SELECT * FROM td_raw_orders
WHERE Delivery_Address IS NULL AND Order_Status = 'Delivered';

-- Промяна на статус на "Pending"
UPDATE td_raw_orders
SET Order_Status = 'Pending'
WHERE Delivery_Address IS NULL AND Order_Status = 'Delivered';
INSERT INTO td_suspicious_records
SELECT * FROM td_raw_orders
WHERE Customer_ID IS NULL;

UPDATE td_raw_orders
SET Payment_Method = 'Unknown'
WHERE Payment_Method IS NULL;

-- Прехвърляне на записи с невалиден формат на дата
INSERT INTO td_invalid_date_format
SELECT * FROM td_raw_orders
WHERE TRY_TO_DATE(Order_Date, 'YYYY-MM-DD') IS NULL;

-- Коригиране на валидни дати
UPDATE td_raw_orders
SET Order_Date = TO_VARCHAR(TO_DATE(Order_Date, 'MM/DD/YYYY'), 'YYYY-MM-DD')
WHERE TRY_TO_DATE(Order_Date, 'MM/DD/YYYY') IS NOT NULL;

-- Прехвърляне на невалидни записи
INSERT INTO td_negative_values
SELECT * FROM td_raw_orders
WHERE Quantity <= 0 OR Price <= 0;

-- Изтриване от основната таблица
DELETE FROM td_raw_orders
WHERE Quantity <= 0 OR Price <= 0;


UPDATE td_raw_orders
SET Discount = 0
WHERE Discount < 0;

UPDATE td_raw_orders
SET Discount = 50
WHERE Discount > 50;

UPDATE td_raw_orders
SET Final_Price = Quantity * Price * (1 - Discount / 100);



-- Създаваме таблица без дублирани редове по ключовите колони
CREATE OR REPLACE TABLE td_raw_orders_deduped AS
SELECT Order_ID, Customer_ID, Delivery_Address, Payment_Method,
       Quantity, Price, Discount, Final_Price, Order_Status, Order_Date
FROM (
  SELECT *, ROW_NUMBER() OVER (
            PARTITION BY Order_ID, Customer_ID, Delivery_Address, Order_Date
            ORDER BY Order_ID) AS rn
  FROM td_raw_orders
)
WHERE rn = 1;

-- Заместваме старата таблица с прочистената
CREATE OR REPLACE TABLE td_raw_orders AS
SELECT * FROM td_raw_orders_deduped;




INSERT INTO td_clean_records
SELECT * FROM td_raw_orders
WHERE Order_ID NOT IN (
    SELECT Order_ID FROM td_for_review
    UNION
    SELECT Order_ID FROM td_suspicious_records
    UNION
    SELECT Order_ID FROM td_invalid_date_format
    UNION
    SELECT Order_ID FROM td_negative_values
);
SHOW TABLES IN "ECOMERSE_DB"."PUBLIC";
SELECT * FROM td_raw_orders LIMIT 10;
SELECT COUNT(*) FROM td_raw_orders;
SHOW TABLES IN "ECOMERSE_DB"."PUBLIC";


SELECT * FROM td_for_review LIMIT 10;
SELECT * FROM td_suspicious_records LIMIT 10;
SELECT * FROM td_invalid_date_format LIMIT 10;
SELECT * FROM td_negative_values LIMIT 10;


SELECT * FROM td_raw_orders LIMIT 10;
SELECT COUNT(*) FROM td_raw_orders;
SELECT * FROM td_raw_orders WHERE Delivery_Address IS NULL AND Order_Status = 'Delivered';
SELECT DISTINCT Delivery_Address, Order_Status FROM td_raw_orders LIMIT 10;
SELECT COUNT(*) FROM td_for_review;
SELECT COUNT(*) FROM td_suspicious_records;
SELECT COUNT(*) FROM td_invalid_date_format;
SELECT COUNT(*) FROM order_id;
SELECT DISTINCT Order_Date FROM td_raw_orders LIMIT 10;
SELECT Order_ID, COUNT(*) 
FROM td_raw_orders
GROUP BY Order_ID
HAVING COUNT(*) > 1;
SELECT * FROM td_raw_orders WHERE Delivery_Address IS NULL;
SELECT * FROM td_raw_orders WHERE Order_Status = 'Delivered';


SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();

INSERT INTO td_clean_records
SELECT * FROM raw_orders;




  
