-- Refactored script to alter the data types of the tables

-- Task 1: orders_types data type conversion
ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid,
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
ALTER COLUMN card_number TYPE VARCHAR(19) USING card_number::varchar,
ALTER COLUMN store_code TYPE VARCHAR(12) USING store_code::varchar,
ALTER COLUMN product_code TYPE VARCHAR(11) USING product_code::varchar,
ALTER COLUMN product_quantity TYPE SMALLINT USING product_quantity::smallint;

-- Task 2: dim_users data type conversion
ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255) USING first_name::varchar,
ALTER COLUMN last_name TYPE VARCHAR(255) USING last_name::varchar,
ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::date,
ALTER COLUMN country_code TYPE VARCHAR(2) USING country_code::varchar,
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
ALTER COLUMN join_date TYPE DATE USING join_date::date;

-- Task 3 (part 2): dim_store_details - data type conversion
ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE float USING (longitude::float),
ALTER COLUMN locality TYPE VARCHAR(255) USING locality::varchar,
ALTER COLUMN store_code TYPE VARCHAR(12) USING store_code::varchar,
ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::smallint,
ALTER COLUMN opening_date TYPE DATE USING opening_date::date,
ALTER COLUMN store_type TYPE VARCHAR(255) USING store_type::varchar,
ALTER COLUMN latitude TYPE float USING (latitude::float),
ALTER COLUMN country_code TYPE VARCHAR(2) USING country_code::varchar,
ALTER COLUMN continent TYPE VARCHAR(255) USING continent::varchar;

-- Task 3 (part 1) : dim_store_details - Change Web Store row value from null to N/A
-- UPDATE dim_store_details SET latitude = 'N/A', longitude = 'N/A' WHERE store_type = 'Web Portal';


-- Task 4: dim_products extra cleaning and add new column 'weight_class'
ALTER TABLE dim_products
ALTER COLUMN product_price TYPE float USING (REPLACE(product_price, '£', '')::float);
ALTER TABLE dim_products ADD COLUMN weight_class VARCHAR;
UPDATE dim_products SET weight_class = 
    CASE 
        WHEN weight < 2 THEN 'Light'
        WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
        WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
        WHEN weight >= 140 THEN 'Truck_Required'
    END;

-- Task 5: dim_products data type conversion and rename column 'removed' to 'still_available'
ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;

ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT,
ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT,
ALTER COLUMN "EAN" TYPE VARCHAR(17),
ALTER COLUMN product_code TYPE VARCHAR(11),
ALTER COLUMN date_added TYPE DATE USING date_added::DATE,
ALTER COLUMN uuid TYPE UUID USING uuid::UUID,
ALTER COLUMN still_available TYPE BOOL USING 
    CASE
    WHEN still_available='Still_available' THEN TRUE
    WHEN still_available='Removed' THEN FALSE
    END,
ALTER COLUMN weight_class TYPE VARCHAR(14);


-- Task 6: dim_date_times data type conversion
ALTER TABLE dim_date_times
  ALTER COLUMN month TYPE VARCHAR(2),
  ALTER COLUMN year TYPE VARCHAR(4),
  ALTER COLUMN day TYPE VARCHAR(2),
  ALTER COLUMN time_period TYPE VARCHAR(10),
  ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;

-- Task 7: dim_card_details data type conversion
ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(19) USING card_number::VARCHAR(19),
ALTER COLUMN expiry_date TYPE VARCHAR(10) USING expiry_date::VARCHAR(10),
ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::DATE;

-- Task 8: Add primary keys to each of the tables prefixed with 'dim'
ALTER TABLE dim_users ADD PRIMARY KEY (user_uuid);
ALTER TABLE dim_store_details ADD PRIMARY KEY (store_code);
ALTER TABLE dim_products ADD PRIMARY KEY (product_code);
ALTER TABLE dim_date_times ADD PRIMARY KEY (date_uuid);
ALTER TABLE dim_card_details ADD PRIMARY KEY (card_number);

-- Task 9: Finalising the star-based schema & adding the foreign keys to the orders_table
ALTER TABLE orders_table
ADD FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid),
ADD FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code),
ADD FOREIGN KEY (product_code) REFERENCES dim_products(product_code),
ADD FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid),
ADD FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);
