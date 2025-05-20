package com.work.bigdatasparkproject.jobs;

import com.work.bigdatasparkproject.config.SparkConfigForTransform;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class ETLtoSnowflakeJob {
    public static void main(String[] args) {
        // Инициализация SparkSession
        try (SparkSession spark = SparkConfigForTransform.createSparkSession()) {

            // Параметры подключения к PostgreSQL
            String pgUrl = "jdbc:postgresql://postgres:5432/sales_db";
            String pgUser = "admin";
            String pgPassword = "secret";

            try {
                // Чтение исходных данных из таблицы mock_data
                Dataset<Row> mockData = spark.read()
                        .format("jdbc")
                        .option("url", pgUrl)
                        .option("dbtable", "mock_data")
                        .option("user", pgUser)
                        .option("password", pgPassword)
                        .option("driver", "org.postgresql.Driver")
                        .load();

                // Кэширование данных для оптимизации
                mockData.cache();
                mockData.createOrReplaceTempView("mock_data");

                // 1. Заполнение dim_customer
                System.out.println("Заполнение dim_customer...");
                Dataset<Row> dimCustomer = spark.sql(
                        "SELECT DISTINCT customer_first_name AS first_name, " +
                                "customer_last_name AS last_name, " +
                                "CAST(customer_age AS INTEGER) AS age " +
                                "FROM mock_data"
                );
                dimCustomer.write()
                        .format("jdbc")
                        .option("url", pgUrl)
                        .option("dbtable", "dim_customer")
                        .option("user", pgUser)
                        .option("password", pgPassword)
                        .option("driver", "org.postgresql.Driver")
                        .mode(SaveMode.Append)
                        .save();

                // Чтение dim_customer с ID
                Dataset<Row> dimCustomerWithId = spark.read()
                        .format("jdbc")
                        .option("url", pgUrl)
                        .option("dbtable", "dim_customer")
                        .option("user", pgUser)
                        .option("password", pgPassword)
                        .option("driver", "org.postgresql.Driver")
                        .load();
                dimCustomerWithId.createOrReplaceTempView("dim_customer");

                // 2. Заполнение customer_contact_info
                System.out.println("Заполнение customer_contact_info...");
                Dataset<Row> customerContactInfo = spark.sql(
                        "SELECT c.customer_id, m.customer_email, m.customer_country, m.customer_postal_code " +
                                "FROM mock_data m " +
                                "JOIN dim_customer c ON m.customer_first_name = c.first_name " +
                                "AND m.customer_last_name = c.last_name " +
                                "AND CAST(m.customer_age AS INTEGER) = c.age"
                );
                customerContactInfo.write()
                        .format("jdbc")
                        .option("url", pgUrl)
                        .option("dbtable", "customer_contact_info")
                        .option("user", pgUser)
                        .option("password", pgPassword)
                        .option("driver", "org.postgresql.Driver")
                        .mode(SaveMode.Append)
                        .save();

                // 3. Заполнение customer_pet_info
                System.out.println("Заполнение customer_pet_info...");
                Dataset<Row> customerPetInfo = spark.sql(
                        "SELECT c.customer_id, m.customer_pet_type AS pet_type, m.customer_pet_name AS pet_name, " +
                                "m.customer_pet_breed AS pet_breed " +
                                "FROM mock_data m " +
                                "JOIN dim_customer c ON m.customer_first_name = c.first_name " +
                                "AND m.customer_last_name = c.last_name " +
                                "AND CAST(m.customer_age AS INTEGER) = c.age"
                );
                customerPetInfo.write()
                        .format("jdbc")
                        .option("url", pgUrl)
                        .option("dbtable", "customer_pet_info")
                        .option("user", pgUser)
                        .option("password", pgPassword)
                        .option("driver", "org.postgresql.Driver")
                        .mode(SaveMode.Append)
                        .save();

                // 4. Заполнение dim_seller
                System.out.println("Заполнение dim_seller...");
                Dataset<Row> dimSeller = spark.sql(
                        "SELECT DISTINCT seller_first_name, seller_last_name " +
                                "FROM mock_data"
                );
                dimSeller.write()
                        .format("jdbc")
                        .option("url", pgUrl)
                        .option("dbtable", "dim_seller")
                        .option("user", pgUser)
                        .option("password", pgPassword)
                        .option("driver", "org.postgresql.Driver")
                        .mode(SaveMode.Append)
                        .save();

                // Чтение dim_seller с ID
                Dataset<Row> dimSellerWithId = spark.read()
                        .format("jdbc")
                        .option("url", pgUrl)
                        .option("dbtable", "dim_seller")
                        .option("user", pgUser)
                        .option("password", pgPassword)
                        .option("driver", "org.postgresql.Driver")
                        .load();
                dimSellerWithId.createOrReplaceTempView("dim_seller");

                // 5. Заполнение seller_contact_info
                System.out.println("Заполнение seller_contact_info...");
                Dataset<Row> sellerContactInfo = spark.sql(
                        "SELECT s.seller_id, m.seller_email, m.seller_country, m.seller_postal_code " +
                                "FROM mock_data m " +
                                "JOIN dim_seller s ON m.seller_first_name = s.seller_first_name " +
                                "AND m.seller_last_name = s.seller_last_name"
                );
                sellerContactInfo.write()
                        .format("jdbc")
                        .option("url", pgUrl)
                        .option("dbtable", "seller_contact_info")
                        .option("user", pgUser)
                        .option("password", pgPassword)
                        .option("driver", "org.postgresql.Driver")
                        .mode(SaveMode.Append)
                        .save();

                // 6. Заполнение product_categories
                System.out.println("Заполнение product_categories...");
                Dataset<Row> productCategories = spark.sql(
                        "SELECT DISTINCT product_category AS category_name " +
                                "FROM mock_data"
                );
                productCategories.write()
                        .format("jdbc")
                        .option("url", pgUrl)
                        .option("dbtable", "product_categories")
                        .option("user", pgUser)
                        .option("password", pgPassword)
                        .option("driver", "org.postgresql.Driver")
                        .mode(SaveMode.Append)
                        .save();

                // Чтение product_categories с ID
                Dataset<Row> productCategoriesWithId = spark.read()
                        .format("jdbc")
                        .option("url", pgUrl)
                        .option("dbtable", "product_categories")
                        .option("user", pgUser)
                        .option("password", pgPassword)
                        .option("driver", "org.postgresql.Driver")
                        .load();
                productCategoriesWithId.createOrReplaceTempView("product_categories");

                // 7. Заполнение dim_products
                System.out.println("Заполнение dim_products...");
                Dataset<Row> dimProducts = spark.sql(
                        "SELECT DISTINCT m.product_name, " +
                                "CAST(m.product_price AS FLOAT) AS product_price, " +
                                "pc.category_id AS product_category, " +
                                "m.customer_pet_type AS pet_category, " +
                                "CAST(m.product_weight AS FLOAT) AS product_weight, " +
                                "m.product_color, m.product_size, m.product_material, " +
                                "m.product_brand, m.product_description " +
                                "FROM mock_data m " +
                                "JOIN product_categories pc ON m.product_category = pc.category_name"
                );
                dimProducts.write()
                        .format("jdbc")
                        .option("url", pgUrl)
                        .option("dbtable", "dim_products")
                        .option("user", pgUser)
                        .option("password", pgPassword)
                        .option("driver", "org.postgresql.Driver")
                        .mode(SaveMode.Append)
                        .save();

                // Чтение dim_products с ID
                Dataset<Row> dimProductsWithId = spark.read()
                        .format("jdbc")
                        .option("url", pgUrl)
                        .option("dbtable", "dim_products")
                        .option("user", pgUser)
                        .option("password", pgPassword)
                        .option("driver", "org.postgresql.Driver")
                        .load();
                dimProductsWithId.createOrReplaceTempView("dim_products");

                // 8. Заполнение product_statistics
                System.out.println("Заполнение product_statistics...");
                Dataset<Row> productStatistics = spark.sql(
                        "SELECT p.product_id, " +
                                "CAST(m.product_rating AS FLOAT) AS product_rating, " +
                                "CAST(m.product_reviews AS INTEGER) AS product_reviews, " +
                                "TO_DATE(m.product_release_date, 'MM/dd/yyyy') AS product_release_date, " +
                                "TO_DATE(m.product_expiry_date, 'MM/dd/yyyy') AS product_expiry_date " +
                                "FROM mock_data m " +
                                "JOIN dim_products p ON m.product_name = p.product_name " +
                                "AND CAST(m.product_price AS FLOAT) = p.product_price"
                ).dropDuplicates("product_id");
                productStatistics.write()
                        .format("jdbc")
                        .option("url", pgUrl)
                        .option("dbtable", "product_statistics")
                        .option("user", pgUser)
                        .option("password", pgPassword)
                        .option("driver", "org.postgresql.Driver")
                        .mode(SaveMode.Append)
                        .save();

                // 9. Заполнение dim_store
                System.out.println("Заполнение dim_store...");
                Dataset<Row> dimStore = spark.sql(
                        "SELECT DISTINCT store_name, store_location, store_city " +
                                "FROM mock_data"
                );
                dimStore.write()
                        .format("jdbc")
                        .option("url", pgUrl)
                        .option("dbtable", "dim_store")
                        .option("user", pgUser)
                        .option("password", pgPassword)
                        .option("driver", "org.postgresql.Driver")
                        .mode(SaveMode.Append)
                        .save();

                // Чтение dim_store с ID
                Dataset<Row> dimStoreWithId = spark.read()
                        .format("jdbc")
                        .option("url", pgUrl)
                        .option("dbtable", "dim_store")
                        .option("user", pgUser)
                        .option("password", pgPassword)
                        .option("driver", "org.postgresql.Driver")
                        .load();
                dimStoreWithId.createOrReplaceTempView("dim_store");

                // 10. Заполнение store_info
                System.out.println("Заполнение store_info...");
                Dataset<Row> storeInfo = spark.sql(
                        "SELECT s.store_id, m.store_state, m.store_country, m.store_phone, m.store_email " +
                                "FROM mock_data m " +
                                "JOIN dim_store s ON m.store_name = s.store_name " +
                                "AND m.store_location = s.store_location " +
                                "AND m.store_city = s.store_city"
                );
                storeInfo.write()
                        .format("jdbc")
                        .option("url", pgUrl)
                        .option("dbtable", "store_info")
                        .option("user", pgUser)
                        .option("password", pgPassword)
                        .option("driver", "org.postgresql.Driver")
                        .mode(SaveMode.Append)
                        .save();

                // 11. Заполнение dim_supplier
                System.out.println("Заполнение dim_supplier...");
                Dataset<Row> dimSupplier = spark.sql(
                        "SELECT DISTINCT supplier_contact, supplier_city, supplier_address " +
                                "FROM mock_data"
                );
                dimSupplier.write()
                        .format("jdbc")
                        .option("url", pgUrl)
                        .option("dbtable", "dim_supplier")
                        .option("user", pgUser)
                        .option("password", pgPassword)
                        .option("driver", "org.postgresql.Driver")
                        .mode(SaveMode.Append)
                        .save();

                // Чтение dim_supplier с ID
                Dataset<Row> dimSupplierWithId = spark.read()
                        .format("jdbc")
                        .option("url", pgUrl)
                        .option("dbtable", "dim_supplier")
                        .option("user", pgUser)
                        .option("password", pgPassword)
                        .option("driver", "org.postgresql.Driver")
                        .load();
                dimSupplierWithId.createOrReplaceTempView("dim_supplier");

                // 12. Заполнение supplier_info
                System.out.println("Заполнение supplier_info...");
                Dataset<Row> supplierInfo = spark.sql(
                        "SELECT s.supplier_id, m.supplier_email, m.supplier_phone, m.supplier_country " +
                                "FROM mock_data m " +
                                "JOIN dim_supplier s ON m.supplier_contact = s.supplier_contact " +
                                "AND m.supplier_city = s.supplier_city " +
                                "AND m.supplier_address = s.supplier_address"
                );
                supplierInfo.write()
                        .format("jdbc")
                        .option("url", pgUrl)
                        .option("dbtable", "supplier_info")
                        .option("user", pgUser)
                        .option("password", pgPassword)
                        .option("driver", "org.postgresql.Driver")
                        .mode(SaveMode.Append)
                        .save();

                // 13. Заполнение fact_sales
                System.out.println("Заполнение fact_sales...");
                Dataset<Row> factSales = spark.sql(
                        "SELECT c.customer_id, p.product_id, s.seller_id, st.store_id, sup.supplier_id, " +
                                "TO_DATE(m.sale_date, 'MM/dd/yyyy') AS sale_date, " +
                                "CAST(m.sale_quantity AS INTEGER) AS product_quantity, " +
                                "CAST(m.sale_total_price AS DECIMAL) AS total_amount " +
                                "FROM mock_data m " +
                                "JOIN dim_customer c ON m.customer_first_name = c.first_name " +
                                "AND m.customer_last_name = c.last_name " +
                                "AND CAST(m.customer_age AS INTEGER) = c.age " +
                                "JOIN dim_products p ON m.product_name = p.product_name " +
                                "AND CAST(m.product_price AS FLOAT) = p.product_price " +
                                "JOIN dim_seller s ON m.seller_first_name = s.seller_first_name " +
                                "AND m.seller_last_name = s.seller_last_name " +
                                "JOIN dim_store st ON m.store_name = st.store_name " +
                                "AND m.store_location = st.store_location " +
                                "AND m.store_city = st.store_city " +
                                "JOIN dim_supplier sup ON m.supplier_contact = sup.supplier_contact " +
                                "AND m.supplier_city = sup.supplier_city " +
                                "AND m.supplier_address = sup.supplier_address"
                );
                factSales.write()
                        .format("jdbc")
                        .option("url", pgUrl)
                        .option("dbtable", "fact_sales")
                        .option("user", pgUser)
                        .option("password", pgPassword)
                        .option("driver", "org.postgresql.Driver")
                        .mode(SaveMode.Append)
                        .save();

                // Очистка кэша и завершение
                mockData.unpersist();
                System.out.println("ETL-процесс успешно завершен");
            } catch (Exception e) {
                System.err.println("Ошибка при выполнении ETL: " + e.getMessage());
                e.printStackTrace();
            } finally {
                spark.stop();
            }
        }
    }
}