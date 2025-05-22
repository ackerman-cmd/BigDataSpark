package com.work.bigdatasparkproject.jobs;

import com.work.bigdatasparkproject.config.SparkConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.logging.Logger;

public class CassandraReportsJob {

    static final Logger log = Logger.getLogger(CassandraReportsJob.class.getName());
    public static void main(String[] args) {

        try (SparkSession spark = SparkConfig.createSparkSession()) {

            spark.conf().set("spark.sql.legacy.timeParserPolicy", "LEGACY");


            String pgUrl = "jdbc:postgresql://postgres:5432/sales_db";
            String pgUser = "admin";
            String pgPassword = "secret";


            spark.conf().set("spark.cassandra.connection.host", "cassandra");
            spark.conf().set("spark.cassandra.connection.port", "9042");
            spark.conf().set("spark.cassandra.auth.username", "cassandra");
            spark.conf().set("spark.cassandra.auth.password", "cassandra");

            try {

                Dataset<Row> factSales = spark.read()
                        .format("jdbc")
                        .option("url", pgUrl)
                        .option("dbtable", "fact_sales")
                        .option("user", pgUser)
                        .option("password", pgPassword)
                        .option("driver", "org.postgresql.Driver")
                        .load();
                factSales.createOrReplaceTempView("fact_sales");

                Dataset<Row> dimProducts = spark.read()
                        .format("jdbc")
                        .option("url", pgUrl)
                        .option("dbtable", "dim_products")
                        .option("user", pgUser)
                        .option("password", pgPassword)
                        .option("driver", "org.postgresql.Driver")
                        .load();
                dimProducts.createOrReplaceTempView("dim_products");

                Dataset<Row> productCategories = spark.read()
                        .format("jdbc")
                        .option("url", pgUrl)
                        .option("dbtable", "product_categories")
                        .option("user", pgUser)
                        .option("password", pgPassword)
                        .option("driver", "org.postgresql.Driver")
                        .load();
                productCategories.createOrReplaceTempView("product_categories");

                Dataset<Row> dimCustomer = spark.read()
                        .format("jdbc")
                        .option("url", pgUrl)
                        .option("dbtable", "dim_customer")
                        .option("user", pgUser)
                        .option("password", pgPassword)
                        .option("driver", "org.postgresql.Driver")
                        .load();
                dimCustomer.createOrReplaceTempView("dim_customer");

                Dataset<Row> customerContactInfo = spark.read()
                        .format("jdbc")
                        .option("url", pgUrl)
                        .option("dbtable", "customer_contact_info")
                        .option("user", pgUser)
                        .option("password", pgPassword)
                        .option("driver", "org.postgresql.Driver")
                        .load();
                customerContactInfo.createOrReplaceTempView("customer_contact_info");

                Dataset<Row> dimStore = spark.read()
                        .format("jdbc")
                        .option("url", pgUrl)
                        .option("dbtable", "dim_store")
                        .option("user", pgUser)
                        .option("password", pgPassword)
                        .option("driver", "org.postgresql.Driver")
                        .load();
                dimStore.createOrReplaceTempView("dim_store");

                Dataset<Row> storeInfo = spark.read()
                        .format("jdbc")
                        .option("url", pgUrl)
                        .option("dbtable", "store_info")
                        .option("user", pgUser)
                        .option("password", pgPassword)
                        .option("driver", "org.postgresql.Driver")
                        .load();
                storeInfo.createOrReplaceTempView("store_info");

                Dataset<Row> dimSupplier = spark.read()
                        .format("jdbc")
                        .option("url", pgUrl)
                        .option("dbtable", "dim_supplier")
                        .option("user", pgUser)
                        .option("password", pgPassword)
                        .option("driver", "org.postgresql.Driver")
                        .load();
                dimSupplier.createOrReplaceTempView("dim_supplier");

                Dataset<Row> supplierInfo = spark.read()
                        .format("jdbc")
                        .option("url", pgUrl)
                        .option("dbtable", "supplier_info")
                        .option("user", pgUser)
                        .option("password", pgPassword)
                        .option("driver", "org.postgresql.Driver")
                        .load();
                supplierInfo.createOrReplaceTempView("supplier_info");

                Dataset<Row> productStatistics = spark.read()
                        .format("jdbc")
                        .option("url", pgUrl)
                        .option("dbtable", "product_statistics")
                        .option("user", pgUser)
                        .option("password", pgPassword)
                        .option("driver", "org.postgresql.Driver")
                        .load();
                productStatistics.createOrReplaceTempView("product_statistics");

                // 1. Витрина продаж по продуктам
                System.out.println("Создание витрины продаж по продуктам...");
                Dataset<Row> salesByProduct = spark.sql(
                        "SELECT p.product_id, p.product_name, pc.category_name, " +
                                "SUM(fs.total_amount) AS total_revenue, " +
                                "SUM(fs.product_quantity) AS total_quantity, " +
                                "AVG(ps.product_rating) AS avg_rating, " +
                                "SUM(ps.product_reviews) AS total_reviews " +
                                "FROM fact_sales fs " +
                                "JOIN dim_products p ON fs.product_id = p.product_id " +
                                "JOIN product_categories pc ON p.product_category = pc.category_id " +
                                "JOIN product_statistics ps ON p.product_id = ps.product_id " +
                                "GROUP BY p.product_id, p.product_name, pc.category_name " +
                                "ORDER BY total_quantity DESC " +
                                "LIMIT 10"
                );
                salesByProduct.show(10);
                salesByProduct.write()
                        .format("org.apache.spark.sql.cassandra")
                        .option("keyspace", "sales")
                        .option("table", "sales_by_product")
                        .mode(SaveMode.Append)
                        .save();

                // 2. Витрина продаж по клиентам
                System.out.println("Создание витрины продаж по клиентам...");
                Dataset<Row> salesByCustomer = spark.sql(
                        "SELECT c.customer_id, c.first_name, c.last_name, cci.customer_country, " +
                                "SUM(fs.total_amount) AS total_amount, " +
                                "AVG(fs.total_amount) AS avg_check " +
                                "FROM fact_sales fs " +
                                "JOIN dim_customer c ON fs.customer_id = c.customer_id " +
                                "JOIN customer_contact_info cci ON c.customer_id = cci.customer_id " +
                                "GROUP BY c.customer_id, c.first_name, c.last_name, cci.customer_country " +
                                "ORDER BY total_amount DESC " +
                                "LIMIT 10"
                );
                salesByCustomer.write()
                        .format("org.apache.spark.sql.cassandra")
                        .option("keyspace", "sales")
                        .option("table", "sales_by_customer")
                        .mode(SaveMode.Append)
                        .save();

                // 3. Витрина продаж по времени
                System.out.println("Создание витрины продаж по времени...");
                Dataset<Row> salesByTime = spark.sql(
                        "SELECT YEAR(fs.sale_date) AS sale_year, MONTH(fs.sale_date) AS sale_month, " +
                                "SUM(fs.total_amount) AS total_revenue, " +
                                "SUM(fs.product_quantity) AS total_quantity, " +
                                "AVG(fs.total_amount) AS avg_order_size " +
                                "FROM fact_sales fs " +
                                "GROUP BY YEAR(fs.sale_date), MONTH(fs.sale_date) " +
                                "ORDER BY sale_year, sale_month"
                );
                salesByTime.write()
                        .format("org.apache.spark.sql.cassandra")
                        .option("keyspace", "sales")
                        .option("table", "sales_by_time")
                        .mode(SaveMode.Append)
                        .save();

                // 4. Витрина продаж по магазинам
                System.out.println("Создание витрины продаж по магазинам...");
                Dataset<Row> salesByStore = spark.sql(
                        "SELECT s.store_id, s.store_name, s.store_city, si.store_country, " +
                                "SUM(fs.total_amount) AS total_revenue, " +
                                "SUM(fs.product_quantity) AS total_quantity, " +
                                "AVG(fs.total_amount) AS avg_check " +
                                "FROM fact_sales fs " +
                                "JOIN dim_store s ON fs.store_id = s.store_id " +
                                "JOIN store_info si ON s.store_id = si.store_id " +
                                "GROUP BY s.store_id, s.store_name, s.store_city, si.store_country " +
                                "ORDER BY total_revenue DESC " +
                                "LIMIT 5"
                );
                salesByStore.write()
                        .format("org.apache.spark.sql.cassandra")
                        .option("keyspace", "sales")
                        .option("table", "sales_by_store")
                        .mode(SaveMode.Append)
                        .save();

                // 5. Витрина продаж по поставщикам
                System.out.println("Создание витрины продаж по поставщикам...");
                Dataset<Row> salesBySupplier = spark.sql(
                        "SELECT sup.supplier_id, sup.supplier_contact, si.supplier_country, " +
                                "SUM(fs.total_amount) AS total_revenue, " +
                                "AVG(p.product_price) AS avg_product_price, " +
                                "SUM(fs.product_quantity) AS total_quantity " +
                                "FROM fact_sales fs " +
                                "JOIN dim_supplier sup ON fs.supplier_id = sup.supplier_id " +
                                "JOIN supplier_info si ON sup.supplier_id = si.supplier_id " +
                                "JOIN dim_products p ON fs.product_id = p.product_id " +
                                "GROUP BY sup.supplier_id, sup.supplier_contact, si.supplier_country " +
                                "ORDER BY total_revenue DESC " +
                                "LIMIT 5"
                );
                salesBySupplier.write()
                        .format("org.apache.spark.sql.cassandra")
                        .option("keyspace", "sales")
                        .option("table", "sales_by_supplier")
                        .mode(SaveMode.Append)
                        .save();

                // 6. Витрина качества продукции
                System.out.println("Создание витрины качества продукции...");
                Dataset<Row> productQuality = spark.sql(
                        "SELECT p.product_id, p.product_name, ps.product_rating, " +
                                "ps.product_reviews AS total_reviews, " +
                                "SUM(fs.total_amount) AS total_revenue, " +
                                "SUM(fs.product_quantity) AS total_quantity " +
                                "FROM fact_sales fs " +
                                "JOIN dim_products p ON fs.product_id = p.product_id " +
                                "JOIN product_statistics ps ON p.product_id = ps.product_id " +
                                "GROUP BY p.product_id, p.product_name, ps.product_rating, ps.product_reviews " +
                                "ORDER BY ps.product_rating DESC"
                );
                productQuality.write()
                        .format("org.apache.spark.sql.cassandra")
                        .option("keyspace", "sales")
                        .option("table", "product_quality")
                        .mode(SaveMode.Append)
                        .save();

                System.out.println("Все витрины успешно созданы в Cassandra");
            } catch (Exception e) {
                System.err.println("Ошибка при создании витрин: " + e.getMessage());
                e.printStackTrace();
            } finally {
                spark.stop();
            }
        }
    }
}