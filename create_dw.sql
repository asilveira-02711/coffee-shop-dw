create or replace table IMPACTA.CAFE.DIM_DATE (
    full_date DATE,
    day_of_week VARCHAR(20),
    day int,
    month int,
	month_name VARCHAR(20),
    year INT,
    quarter INT,
    is_holiday BOOLEAN
);

set total_time = (select DATEDIFF(DAY, '2023-01-01', '2024-01-01'));

INSERT INTO IMPACTA.CAFE.DIM_DATE (
    full_date, day_of_week, day, month, month_name, year, quarter, is_holiday
)
WITH dates_cte AS (
    SELECT DATEADD(DAY, SEQ8(), TO_DATE('2023-01-01')) AS full_date
    FROM TABLE(GENERATOR(ROWCOUNT => $total_time))
)
SELECT 
    full_date,
    DAYNAME(full_date) AS day_of_week,
    DAY(full_date),
    MONTH(full_date) AS month,
    CASE MONTH(full_date)
        WHEN 1 THEN 'January'
        WHEN 2 THEN 'February'
        WHEN 3 THEN 'March'
        WHEN 4 THEN 'April'
        WHEN 5 THEN 'May'
        WHEN 6 THEN 'June'
        WHEN 7 THEN 'July'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END AS month_name,
    YEAR(full_date) AS year,
    CEIL(MONTH(full_date) / 3) AS quarter,
    CASE 
        WHEN full_date IN (
            '2023-01-01','2023-02-20','2023-02-21','2023-04-07',
            '2023-04-21','2023-05-01','2023-06-08','2023-09-07',
            '2023-10-12','2023-11-02','2023-11-15','2023-12-25'
        )
        THEN TRUE ELSE FALSE 
    END AS is_holiday
FROM dates_cte;

create or replace table IMPACTA.CAFE.DIM_PRODUCT (
    product_id int,
    unit_price number(10,2),
    product_category VARCHAR,
    product_type VARCHAR,
    product_detail VARCHAR,
	size VARCHAR
);


create or replace table IMPACTA.CAFE.DIM_STORE (
    store_id int,
    store_location VARCHAR(50)
);

create or replace table IMPACTA.CAFE.FACT_SALES (
    transaction_id NUMBER(38, 0),
    transaction_date DATE,
	transaction_time TIME,
	store_id int,
	product_id int,
	transaction_qty int,
	size VARCHAR,
	total_bill NUMBER(38, 1)
);