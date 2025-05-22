package com.work.bigdatasparkproject.jobs;

import com.work.bigdatasparkproject.config.SparkConfig;
import org.apache.spark.sql.*;

public class Neo4jReportsJob {
    public static void main(String[] args) {
        try (SparkSession spark = SparkConfig.createSparkSession()) {
            spark.conf().set("spark.sql.legacy.timeParserPolicy", "LEGACY");

            // Настройки Neo4j
            String neo4jUrl = "bolt://neo4j-db:7687";
            String neo4jUser = "neo4j";
            String neo4jPassword = "password-secret";
            String neo4jDatabase = "salesdb";

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
                System.out.println("Создание узлов продуктов и категорий...");
                Dataset<Row> productsWithSales = spark.sql(
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

                writeToNeo4j(productsWithSales, neo4jUrl, neo4jUser, neo4jPassword, neo4jDatabase)
                        .option("labels", "Product")
                        .option("node.keys", "product_id")
                        .option("node.properties", "product_name,category_name,total_revenue,total_quantity,avg_rating,total_reviews")
                        .save();

                writeToNeo4j(productCategories, neo4jUrl, neo4jUser, neo4jPassword, neo4jDatabase)
                        .option("labels", "Category")
                        .option("node.keys", "category_id")
                        .save();

                // 2. Витрина: Продажи по клиентам
                System.out.println("Создание узлов клиентов и отношений покупок...");
                Dataset<Row> customers = spark.sql(
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

                writeToNeo4j(customers, neo4jUrl, neo4jUser, neo4jPassword, neo4jDatabase)
                        .option("labels", "Customer")
                        .option("node.keys", "customer_id")
                        .option("node.properties", "first_name,last_name,customer_country,total_amount,avg_check")
                        .save();

                Dataset<Row> customerPurchases = spark.sql(
                        "SELECT c.customer_id, fs.product_id, " +
                                "CAST(SUM(fs.total_amount) AS DOUBLE) AS total_spent " +
                                "FROM fact_sales fs " +
                                "JOIN dim_customer c ON fs.customer_id = c.customer_id " +
                                "GROUP BY c.customer_id, fs.product_id"
                );

                writeToNeo4j(customerPurchases, neo4jUrl, neo4jUser, neo4jPassword, neo4jDatabase)
                        .option("relationship", "PURCHASED")
                        .option("relationship.save.strategy", "KEYS")
                        .option("relationship.source.labels", "Customer")
                        .option("relationship.source.node.keys", "customer_id")
                        .option("relationship.target.labels", "Product")
                        .option("relationship.target.node.keys", "product_id")
                        .option("relationship.properties", "total_spent")
                        .save();

                // 3. Витрина: Продажи по времени
                System.out.println("Создание временных узлов...");
                Dataset<Row> salesByTime = spark.sql(
                        "SELECT YEAR(fs.sale_date) AS sale_year, MONTH(fs.sale_date) AS sale_month, " +
                                "CAST(SUM(fs.total_amount) AS DOUBLE) AS total_revenue, " +
                                "SUM(fs.product_quantity) AS total_quantity, " +
                                "CAST(AVG(fs.total_amount) AS DOUBLE) AS avg_order_size " +
                                "FROM fact_sales fs " +
                                "GROUP BY YEAR(fs.sale_date), MONTH(fs.sale_date) " +
                                "ORDER BY sale_year, sale_month"
                );

                writeToNeo4j(salesByTime, neo4jUrl, neo4jUser, neo4jPassword, neo4jDatabase)
                        .option("labels", "TimePeriod")
                        .option("node.keys", "sale_year,sale_month")
                        .option("node.properties", "total_revenue,total_quantity,avg_order_size")
                        .save();

                // 4. Витрина: Продажи по магазинам
                System.out.println("Создание узлов магазинов...");
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

                writeToNeo4j(salesByStore, neo4jUrl, neo4jUser, neo4jPassword, neo4jDatabase)
                        .option("labels", "Store")
                        .option("node.keys", "store_id")
                        .option("node.properties", "store_name,store_city,store_country,total_revenue,total_quantity,avg_check")
                        .save();

                // 5. Витрина: Продажи по поставщикам
                System.out.println("Создание узлов поставщиков...");
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

                writeToNeo4j(salesBySupplier, neo4jUrl, neo4jUser, neo4jPassword, neo4jDatabase)
                        .option("labels", "Supplier")
                        .option("node.keys", "supplier_id")
                        .option("node.properties", "supplier_contact,supplier_country,total_revenue,avg_product_price,total_quantity")
                        .save();

                // 6. Витрина: Качество продукции
                System.out.println("Создание свойств качества продуктов...");
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

                writeToNeo4j(productQuality, neo4jUrl, neo4jUser, neo4jPassword, neo4jDatabase)
                        .option("labels", "Product")
                        .option("node.keys", "product_id")
                        .option("node.properties", "product_name,product_rating,total_reviews,total_revenue,total_quantity")
                        .save();

                System.out.println("Все витрины успешно созданы в Neo4j");

            } catch (Exception e) {
                System.err.println("Ошибка при создании витрин: " + e.getMessage());
                e.printStackTrace();
                throw new RuntimeException("Не удалось создать витрины в Neo4j", e);
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

    private static DataFrameWriter<Row> writeToNeo4j(Dataset<Row> df, String url, String user, String password, String database) {
        return df.write()
                .format("org.neo4j.spark.DataSource")
                .option("url", url)
                .option("authentication.type", "basic")
                .option("authentication.basic.username", user)
                .option("authentication.basic.password", password)
                .option("database", database)
                .option("batch.size", "1000")
                .mode(SaveMode.Overwrite);
    }
}