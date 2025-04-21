

-- 1. Заполнение таблицы клиентов (dim_customer)
INSERT INTO dim_customer (first_name, last_name, age)
SELECT DISTINCT
    customer_first_name,
    customer_last_name,
    customer_age::INT
FROM mock_data;

-- 2. Заполнение контактной информации клиентов
INSERT INTO customer_contact_info (customer_id, customer_email, customer_country, customer_postal_code)
SELECT
    c.customer_id,
    m.customer_email,
    m.customer_country,
    m.customer_postal_code
FROM mock_data m
         JOIN dim_customer c ON m.customer_first_name = c.first_name
    AND m.customer_last_name = c.last_name
    AND m.customer_age::INT = c.age;


-- 3. Заполнение информации о питомцах клиентов (customer_pet_info)
INSERT INTO customer_pet_info (customer_id, pet_type, pet_name, pet_breed)
SELECT
    c.customer_id,
    m.customer_pet_type,
    m.customer_pet_name,
    m.customer_pet_breed
FROM mock_data m
         JOIN dim_customer c ON m.customer_first_name = c.first_name
    AND m.customer_last_name = c.last_name
    AND m.customer_age::INT = c.age;

-- 4. Заполнение таблицы продавцов (dim_seller)
INSERT INTO dim_seller (seller_first_name, seller_last_name)
SELECT DISTINCT
    seller_first_name,
    seller_last_name
FROM mock_data;

-- 5. Заполнение контактной информации продавцов (seller_contact_info)
INSERT INTO seller_contact_info (seller_id, seller_email, seller_country, seller_postal_code)
SELECT
    s.seller_id,
    m.seller_email,
    m.seller_country,
    m.seller_postal_code
FROM mock_data m
         JOIN dim_seller s ON m.seller_first_name = s.seller_first_name
    AND m.seller_last_name = s.seller_last_name;

-- 6. Заполнение категорий товаров (product_categories)
INSERT INTO product_categories (category_name)
SELECT DISTINCT product_category FROM mock_data;

-- 7. Заполнение таблицы товаров (dim_products)
INSERT INTO dim_products (
    product_name,
    product_price,
    product_category,
    pet_category,
    product_weight,
    product_color,
    product_size,
    product_material,
    product_brand,
    product_description
)
SELECT DISTINCT
    m.product_name,
    m.product_price::DECIMAL(10,2),
    pc.category_id,
    m.customer_pet_type,
    m.product_weight::FLOAT,
    m.product_color,
    m.product_size,
    m.product_material,
    m.product_brand,
    m.product_description
FROM mock_data m
         JOIN product_categories pc ON m.product_category = pc.category_name;

-- 8. Заполнение статистики товаров (product_statistics) - ИСПРАВЛЕНО
INSERT INTO product_statistics (
    product_id,
    product_rating,
    product_reviews,
    product_release_date,
    product_expiry_date
)
SELECT DISTINCT ON (p.product_id)
    p.product_id,
    m.product_rating::FLOAT,
    m.product_reviews::INT,
    TO_DATE(m.product_release_date, 'MM/DD/YYYY'),
    TO_DATE(m.product_expiry_date, 'MM/DD/YYYY')
FROM mock_data m
         JOIN dim_products p ON m.product_name = p.product_name
    AND m.product_price::DECIMAL(10,2) = p.product_price
ON CONFLICT (product_id) DO NOTHING;

-- 9. Заполнение таблицы магазинов (dim_store)
INSERT INTO dim_store (store_name, store_location, store_city)
SELECT DISTINCT
    store_name,
    store_location,
    store_city
FROM mock_data;

-- 10. Заполнение информации о магазинах (store_info)
INSERT INTO store_info (
    store_id,
    store_state,
    store_country,
    store_phone,
    store_email
)
SELECT
    s.store_id,
    m.store_state,
    m.store_country,
    m.store_phone,
    m.store_email
FROM mock_data m
         JOIN dim_store s ON m.store_name = s.store_name and m.store_location = s.store_location and m.store_city = s.store_city ;


-- 11. Заполнение таблицы поставщиков (dim_supplier) - БЕЗ supplier_name
INSERT INTO dim_supplier (supplier_contact, supplier_city, supplier_address)
SELECT DISTINCT
    m.supplier_contact ,
    m.supplier_city,
    m.supplier_address
FROM mock_data m;

-- 12. Заполнение информации о поставщиках (supplier_info) - БЕЗ supplier_name
INSERT INTO supplier_info (
    supplier_id,
    supplier_email,
    supplier_phone,
    supplier_country
)
SELECT
    s.supplier_id,
    m.supplier_email,
    m.supplier_phone,
    m.supplier_country
FROM mock_data m
         JOIN dim_supplier s ON m.supplier_contact = s.supplier_contact and m.supplier_city = s.supplier_city
    and m.supplier_address = s.supplier_address;

-- 13. Заполнение фактов продаж (fact_sales) - БЕЗ supplier_name
INSERT INTO fact_sales (
    customer_id,
    product_id,
    seller_id,
    store_id,
    supplier_id,
    sale_date,
    product_quantity,
    total_amount
)
SELECT
    c.customer_id,
    p.product_id,
    s.seller_id,
    st.store_id,
    sup.supplier_id,
    TO_DATE(m.sale_date, 'MM/DD/YYYY'),
    m.sale_quantity::INT,
    m.sale_total_price::DECIMAL(12,2)
FROM mock_data m
         JOIN dim_customer c ON m.customer_first_name = c.first_name
    AND m.customer_last_name = c.last_name
    AND m.customer_age::INT = c.age
         JOIN dim_products p ON m.product_name = p.product_name and m.product_price = p.product_price and m.product_weight = p.product_weight
         JOIN dim_seller s ON m.seller_first_name = s.seller_first_name
    AND m.seller_last_name = s.seller_last_name
         JOIN dim_store st ON m.store_name = st.store_name and m.store_location = st.store_location and m.store_city = st.store_city
         JOIN dim_supplier sup ON m.supplier_address = sup.supplier_address and m.supplier_contact = sup.supplier_contact
    AND m.supplier_city = sup.supplier_city;

