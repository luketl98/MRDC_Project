-- t1: orders_types data type conversion

ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;

ALTER TABLE orders_table 
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid;

ALTER TABLE orders_table 
ALTER COLUMN card_number TYPE VARCHAR(19) USING card_number::varchar;

ALTER TABLE orders_table
ALTER COLUMN store_code TYPE VARCHAR(12) USING store_code::varchar;

ALTER TABLE orders_table
ALTER COLUMN product_code TYPE VARCHAR(11) USING product_code::varchar;

ALTER TABLE orders_table 
ALTER COLUMN product_quantity TYPE SMALLINT USING product_quantity::smallint;


-- t2: dim_users data type conversion

ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255) USING first_name::varchar;

ALTER TABLE dim_users
ALTER COLUMN last_name TYPE VARCHAR(255) USING last_name::varchar;

ALTER TABLE dim_users
ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::date;

ALTER TABLE dim_users
ALTER COLUMN country_code TYPE VARCHAR(2) USING country_code::varchar;

ALTER TABLE dim_users
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid;

ALTER TABLE dim_users
ALTER COLUMN join_date TYPE DATE USING join_date::date;


-- t3: dim_store_details data type conversion

ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE float USING (longitude::float);

ALTER TABLE dim_store_details
ALTER COLUMN locality TYPE VARCHAR(255) USING locality::varchar;

ALTER TABLE dim_store_details
ALTER COLUMN store_code TYPE VARCHAR(11) USING store_code::varchar;

ALTER TABLE dim_store_details
ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::smallint;

ALTER TABLE dim_store_details
ALTER COLUMN opening_date TYPE DATE USING opening_date::date;

ALTER TABLE dim_store_details
ALTER COLUMN store_type TYPE VARCHAR(255) USING store_type::varchar;

ALTER TABLE dim_store_details
ALTER COLUMN latitude TYPE float USING (latitude::float);

ALTER TABLE dim_store_details
ALTER COLUMN country_code TYPE VARCHAR(2) USING country_code::varchar;

ALTER TABLE dim_store_details
ALTER COLUMN continent TYPE VARCHAR(255) USING continent::varchar;

-- t3 (continuued) : Change Web Store row value from null to N/A

UPDATE dim_store_details
SET latitude = 'N/A'
WHERE store_type = 'Web Portal';

-- t4: dim_products extra cleaning and add new column 'weight_class'

UPDATE dim_products SET product_price = REPLACE(product_price, '£', '')::FLOAT;

ALTER TABLE dim_products ADD COLUMN weight_class VARCHAR;

UPDATE dim_products SET weight_class = 
    CASE 
        WHEN weight < 2 THEN 'Light'
        WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
        WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
        WHEN weight >= 140 THEN 'Truck_Required'
    END;


-- t5: dim_products data type conversion and rename column 'removed' to 'still_available'

ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;

ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT;

ALTER TABLE dim_products
ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT;

ALTER TABLE dim_products
ALTER COLUMN "EAN" TYPE VARCHAR(17);

ALTER TABLE dim_products
ALTER COLUMN product_code TYPE VARCHAR(11);

ALTER TABLE dim_products
ALTER COLUMN date_added TYPE DATE USING date_added::DATE;

ALTER TABLE dim_products
ALTER COLUMN uuid TYPE UUID USING uuid::UUID;

ALTER TABLE dim_products
ALTER COLUMN still_available TYPE BOOL USING 
    CASE
    WHEN still_available='Still_available' THEN TRUE
    WHEN still_available='Removed' THEN FALSE
    END;

ALTER TABLE dim_products
ALTER COLUMN weight_class TYPE VARCHAR(14);


-- t6: dim_date_times data type conversion

ALTER TABLE dim_date_times
  ALTER COLUMN month TYPE VARCHAR(2),
  ALTER COLUMN year TYPE VARCHAR(4),
  ALTER COLUMN day TYPE VARCHAR(2),
  ALTER COLUMN time_period TYPE VARCHAR(10),
  ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;


-- t7: dim_card_details data type conversion

ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(19) USING card_number::VARCHAR(19),
ALTER COLUMN expiry_date TYPE VARCHAR(10) USING expiry_date::VARCHAR(10),
ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::DATE;

