package com.work.bigdatasparkproject.jobs;

import com.work.bigdatasparkproject.config.SparkConfig;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

import java.util.Arrays;

public class ValkeyReportsJob {
    public static void main(String[] args) {
        try (SparkSession spark = SparkConfig.createSparkSession()) {
            spark.conf().set("spark.sql.legacy.timeParserPolicy", "LEGACY");

            // Настройки Valkey
            String valkeyHost = "valkey-db";
            String valkeyPort = "6379";
            String valkeyDb = "1"; // sales_db (индекс 1)

            // Настройки PostgreSQL
            String pgUrl = "jdbc:postgresql://postgres:5432/sales_db";
            String pgUser = "admin";
            String pgPassword = "secret";

            try {
                // Загрузка данных из PostgreSQL
                Dataset<Row> factSales = loadJdbcTable(spark, pgUrl, pgUser, pgPassword, "fact_sales");
                Dataset<Row> dimProducts = loadJdbcTable(spark, pgUrl, pgUser, pgPassword, "dim_products");
                Dataset<Row> productCategories = loadJdbcTable(spark, pgUrl, pgUser, pgPassword, "product_categories");
                Dataset<Row> dimCustomer = loadJdbcTable(spark, pgUrl, pgUser, pgPassword, "dim_customer");
                Dataset<Row> customerContactInfo = loadJdbcTable(spark, pgUrl, pgUser, pgPassword, "customer_contact_info");
                Dataset<Row> dimStore = loadJdbcTable(spark, pgUrl, pgUser, pgPassword, "dim_store");
                Dataset<Row> storeInfo = loadJdbcTable(spark, pgUrl, pgUser, pgPassword, "store_info");
                Dataset<Row> dimSupplier = loadJdbcTable(spark, pgUrl, pgUser, pgPassword, "dim_supplier");
                Dataset<Row> supplierInfo = loadJdbcTable(spark, pgUrl, pgUser, pgPassword, "supplier_info");
                Dataset<Row> productStatistics = loadJdbcTable(spark, pgUrl, pgUser, pgPassword, "product_statistics");

                // Регистрация временных представлений
                factSales.createOrReplaceTempView("fact_sales");
                dimProducts.createOrReplaceTempView("dim_products");
                productCategories.createOrReplaceTempView("product_categories");
                dimCustomer.createOrReplaceTempView("dim_customer");
                customerContactInfo.createOrReplaceTempView("customer_contact_info");
                dimStore.createOrReplaceTempView("dim_store");
                storeInfo.createOrReplaceTempView("store_info");
                dimSupplier.createOrReplaceTempView("dim_supplier");
                supplierInfo.createOrReplaceTempView("supplier_info");
                productStatistics.createOrReplaceTempView("product_statistics");

                // 1. Витрина: Продажи по продуктам
                System.out.println("Создание витрины продаж по продуктам...");
                Dataset<Row> salesByProduct = spark.sql(
                        "SELECT p.product_id, p.product_name, pc.category_name, " +
                                "CAST(SUM(fs.total_amount) AS DOUBLE) AS total_revenue, " +
                                "SUM(fs.product_quantity) AS total_quantity, " +
                                "CAST(AVG(ps.product_rating) AS DOUBLE) AS avg_rating, " +
                                "SUM(ps.product_reviews) AS total_reviews " +
                                "FROM fact_sales fs " +
                                "JOIN dim_products p ON fs.product_id = p.product_id " +
                                "JOIN product_categories pc ON p.product_category = pc.category_id " +
                                "JOIN product_statistics ps ON p.product_id = ps.product_id " +
                                "GROUP BY p.product_id, p.product_name, pc.category_name " +
                                "ORDER BY total_quantity DESC " +
                                "LIMIT 10"
                );
                writeToValkey(salesByProduct, valkeyHost, valkeyPort, valkeyDb, "sales_by_product", new String[]{"product_id"});

                // 2. Витрина: Продажи по клиентам
                System.out.println("Создание витрины продаж по клиентам...");
                Dataset<Row> salesByCustomer = spark.sql(
                        "SELECT c.customer_id, c.first_name, c.last_name, cci.customer_country, " +
                                "CAST(SUM(fs.total_amount) AS DOUBLE) AS total_amount, " +
                                "CAST(AVG(fs.total_amount) AS DOUBLE) AS avg_check " +
                                "FROM fact_sales fs " +
                                "JOIN dim_customer c ON fs.customer_id = c.customer_id " +
                                "JOIN customer_contact_info cci ON c.customer_id = cci.customer_id " +
                                "GROUP BY c.customer_id, c.first_name, c.last_name, cci.customer_country " +
                                "ORDER BY total_amount DESC " +
                                "LIMIT 10"
                );
                writeToValkey(salesByCustomer, valkeyHost, valkeyPort, valkeyDb, "sales_by_customer", new String[]{"customer_id"});

                // 3. Витрина: Продажи по времени
                System.out.println("Создание витрины продаж по времени...");
                Dataset<Row> salesByTime = spark.sql(
                        "SELECT YEAR(fs.sale_date) AS sale_year, MONTH(fs.sale_date) AS sale_month, " +
                                "CAST(SUM(fs.total_amount) AS DOUBLE) AS total_revenue, " +
                                "SUM(fs.product_quantity) AS total_quantity, " +
                                "CAST(AVG(fs.total_amount) AS DOUBLE) AS avg_order_size " +
                                "FROM fact_sales fs " +
                                "GROUP BY YEAR(fs.sale_date), MONTH(fs.sale_date) " +
                                "ORDER BY sale_year, sale_month"
                );
                writeToValkey(salesByTime, valkeyHost, valkeyPort, valkeyDb, "sales_by_time", new String[]{"sale_year", "sale_month"});

                // 4. Витрина: Продажи по магазинам
                System.out.println("Создание витрины продаж по магазинам...");
                Dataset<Row> salesByStore = spark.sql(
                        "SELECT s.store_id, s.store_name, s.store_city, si.store_country, " +
                                "CAST(SUM(fs.total_amount) AS DOUBLE) AS total_revenue, " +
                                "SUM(fs.product_quantity) AS total_quantity, " +
                                "CAST(AVG(fs.total_amount) AS DOUBLE) AS avg_check " +
                                "FROM fact_sales fs " +
                                "JOIN dim_store s ON fs.store_id = s.store_id " +
                                "JOIN store_info si ON s.store_id = si.store_id " +
                                "GROUP BY s.store_id, s.store_name, s.store_city, si.store_country " +
                                "ORDER BY total_revenue DESC " +
                                "LIMIT 5"
                );
                writeToValkey(salesByStore, valkeyHost, valkeyPort, valkeyDb, "sales_by_store", new String[]{"store_id"});

                // 5. Витрина: Продажи по поставщикам
                System.out.println("Создание витрины продаж по поставщикам...");
                Dataset<Row> salesBySupplier = spark.sql(
                        "SELECT sup.supplier_id, sup.supplier_contact, si.supplier_country, " +
                                "CAST(SUM(fs.total_amount) AS DOUBLE) AS total_revenue, " +
                                "CAST(AVG(p.product_price) AS DOUBLE) AS avg_product_price, " +
                                "SUM(fs.product_quantity) AS total_quantity " +
                                "FROM fact_sales fs " +
                                "JOIN dim_supplier sup ON fs.supplier_id = sup.supplier_id " +
                                "JOIN supplier_info si ON sup.supplier_id = si.supplier_id " +
                                "JOIN dim_products p ON fs.product_id = p.product_id " +
                                "GROUP BY sup.supplier_id, sup.supplier_contact, si.supplier_country " +
                                "ORDER BY total_revenue DESC " +
                                "LIMIT 5"
                );
                writeToValkey(salesBySupplier, valkeyHost, valkeyPort, valkeyDb, "sales_by_supplier", new String[]{"supplier_id"});

                // 6. Витрина: Качество продукции
                System.out.println("Создание витрины качества продукции...");
                Dataset<Row> productQuality = spark.sql(
                        "SELECT p.product_id, p.product_name, " +
                                "CAST(ps.product_rating AS DOUBLE) AS product_rating, " +
                                "ps.product_reviews AS total_reviews, " +
                                "CAST(SUM(fs.total_amount) AS DOUBLE) AS total_revenue, " +
                                "SUM(fs.product_quantity) AS total_quantity " +
                                "FROM fact_sales fs " +
                                "JOIN dim_products p ON fs.product_id = p.product_id " +
                                "JOIN product_statistics ps ON p.product_id = ps.product_id " +
                                "GROUP BY p.product_id, p.product_name, ps.product_rating, ps.product_reviews " +
                                "ORDER BY ps.product_rating DESC"
                );
                writeToValkey(productQuality, valkeyHost, valkeyPort, valkeyDb, "product_quality", new String[]{"product_id"});

                System.out.println("Все витрины успешно созданы в Valkey");

            } catch (Exception e) {
                System.err.println("Ошибка при создании витрин: " + e.getMessage());
                e.printStackTrace();
                throw new RuntimeException("Не удалось создать витрины в Valkey", e);
            }
        }
    }

    private static Dataset<Row> loadJdbcTable(SparkSession spark, String url, String user, String password, String table) {
        try {
            return spark.read()
                    .format("jdbc")
                    .option("url", url)
                    .option("dbtable", table)
                    .option("user", user)
                    .option("password", password)
                    .option("driver", "org.postgresql.Driver")
                    .load();
        } catch (Exception e) {
            System.err.println("Ошибка при загрузке таблицы " + table + ": " + e.getMessage());
            throw new RuntimeException("Не удалось загрузить данные из PostgreSQL", e);
        }
    }

    private static void writeToValkey(Dataset<Row> df, String host, String port, String db, String tablePrefix, String[] keyColumns) {
        Dataset<Row> dfToWrite = df;
        String keyColumnName;

        if (keyColumns.length > 1) {
            // Для составных ключей создаем новую колонку, объединяющую значения
            keyColumnName = "key_column";
            // Convert String[] to Column[]
            Column[] keyCols = Arrays.stream(keyColumns)
                    .map(functions::col)
                    .toArray(Column[]::new);
            Column keyCol = concat_ws(":", keyCols);
            dfToWrite = df.withColumn(keyColumnName, keyCol);
        } else {
            // Для одного ключа используем имя существующей колонки
            keyColumnName = keyColumns[0];
        }

        dfToWrite.write()
                .format("org.apache.spark.sql.redis")
                .option("host", host)
                .option("port", port)
                .option("db", db)
                .option("table", tablePrefix)
                .option("key.column", keyColumnName)
                .option("spark.redis.batch.size", "1000") // Оптимизация записи
                .mode(SaveMode.Overwrite)
                .save();
    }
}