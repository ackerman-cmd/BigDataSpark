package com.work.bigdatasparkproject.jobs;

import com.work.bigdatasparkproject.config.SparkConfig;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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
                // Загрузка данных из PostgreSQL (аналогично ClickHouse реализации)
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

                // 1. Витрина: Продукты и категории
                System.out.println("Создание узлов продуктов и категорий...");
                Dataset<Row> productsWithCategories = spark.sql(
                        "SELECT p.product_id, p.product_name, pc.category_name " +
                                "FROM dim_products p " +
                                "JOIN product_categories pc ON p.product_category = pc.category_id"
                );

                writeToNeo4j(productsWithCategories, neo4jUrl, neo4jUser, neo4jPassword, neo4jDatabase)
                        .option("labels", "Product")
                        .option("node.keys", "product_id")
                        .save();

                writeToNeo4j(productCategories, neo4jUrl, neo4jUser, neo4jPassword, neo4jDatabase)
                        .option("labels", "Category")
                        .option("node.keys", "category_id")
                        .save();

                // 2. Витрина: Продажи по клиентам
                System.out.println("Создание узлов клиентов и отношений покупок...");
                Dataset<Row> customers = spark.sql(
                        "SELECT c.customer_id, c.first_name, c.last_name, cci.customer_country " +
                                "FROM dim_customer c " +
                                "JOIN customer_contact_info cci ON c.customer_id = cci.customer_id"
                );

                writeToNeo4j(customers, neo4jUrl, neo4jUser, neo4jPassword, neo4jDatabase)
                        .option("labels", "Customer")
                        .option("node.keys", "customer_id")
                        .save();

                Dataset<Row> customerPurchases = spark.sql(
                        "SELECT fs.customer_id, fs.product_id, SUM(fs.total_amount) as total_spent " +
                                "FROM fact_sales fs " +
                                "GROUP BY fs.customer_id, fs.product_id"
                );

                writeToNeo4j(customerPurchases, neo4jUrl, neo4jUser, neo4jPassword, neo4jDatabase)
                        .option("relationship", "PURCHASED")
                        .option("relationship.source.labels", "Customer")
                        .option("relationship.source.node.keys", "customer_id:customer_id")
                        .option("relationship.target.labels", "Product")
                        .option("relationship.target.node.keys", "product_id:product_id")
                        .option("relationship.properties", "total_spent")
                        .save();

                // 3. Витрина: Продажи по времени
                System.out.println("Создание временных узлов...");
                Dataset<Row> timeDimensions = spark.sql(
                        "SELECT DISTINCT YEAR(sale_date) as year, MONTH(sale_date) as month " +
                                "FROM fact_sales"
                );

                writeToNeo4j(timeDimensions, neo4jUrl, neo4jUser, neo4jPassword, neo4jDatabase)
                        .option("labels", "TimePeriod")
                        .option("node.keys", "year,month")
                        .save();

                // 4. Витрина: Продажи по магазинам
                System.out.println("Создание узлов магазинов...");
                Dataset<Row> stores = spark.sql(
                        "SELECT s.store_id, s.store_name, si.store_country " +
                                "FROM dim_store s " +
                                "JOIN store_info si ON s.store_id = si.store_id"
                );

                writeToNeo4j(stores, neo4jUrl, neo4jUser, neo4jPassword, neo4jDatabase)
                        .option("labels", "Store")
                        .option("node.keys", "store_id")
                        .save();

                // 5. Витрина: Продажи по поставщикам
                System.out.println("Создание узлов поставщиков...");
                Dataset<Row> suppliers = spark.sql(
                        "SELECT sup.supplier_id, sup.supplier_contact, si.supplier_country " +
                                "FROM dim_supplier sup " +
                                "JOIN supplier_info si ON sup.supplier_id = si.supplier_id"
                );

                writeToNeo4j(suppliers, neo4jUrl, neo4jUser, neo4jPassword, neo4jDatabase)
                        .option("labels", "Supplier")
                        .option("node.keys", "supplier_id")
                        .save();

                // 6. Витрина: Качество продукции
                System.out.println("Создание свойств качества продуктов...");
                Dataset<Row> productQuality = spark.sql(
                        "SELECT p.product_id, ps.product_rating, ps.product_reviews " +
                                "FROM dim_products p " +
                                "JOIN product_statistics ps ON p.product_id = ps.product_id"
                );

                writeToNeo4j(productQuality, neo4jUrl, neo4jUser, neo4jPassword, neo4jDatabase)
                        .option("labels", "Product")
                        .option("node.keys", "product_id")
                        .option("node.properties", "product_rating,product_reviews")
                        .mode("Overwrite")
                        .save();

                System.out.println("Все витрины успешно созданы в Neo4j");

            } catch (Exception e) {
                System.err.println("Ошибка при создании витрин: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    private static Dataset<Row> loadJdbcTable(SparkSession spark, String url, String user, String password, String table) {
        return spark.read()
                .format("jdbc")
                .option("url", url)
                .option("dbtable", table)
                .option("user", user)
                .option("password", password)
                .option("driver", "org.postgresql.Driver")
                .load();
    }

    private static DataFrameWriter<Row> writeToNeo4j(Dataset<Row> df, String url, String user, String password, String database) {
        return df.write()
                .format("org.neo4j.spark.DataSource")
                .option("url", url)
                .option("authentication.type", "basic")
                .option("authentication.basic.username", user)
                .option("authentication.basic.password", password)
                .option("database", database)
                .option("batch.size", "1000");
    }
}