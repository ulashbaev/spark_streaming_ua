package biz.svyatoslav.learning.bigdata.spark.streaming;

import biz.svyatoslav.learning.bigdata.spark.streaming.constants.AppConstants;
import biz.svyatoslav.learning.bigdata.spark.streaming.utils.CSVUtils;
import biz.svyatoslav.learning.bigdata.spark.streaming.utils.DateUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class ReceiptStreamingApp {
    public static void main(String[] args) {
        // Initialize Spark Session with additional optimizations
        SparkSession spark = SparkSession.builder()
                .config("spark.hadoop.home.dir", "C:/hadoop")
                .config("spark.sql.warehouse.dir", "file:///C:/temp")
                .appName("RestaurantReceiptsWeatherProcessor")
                .master("local[*]")
                .config("spark.driver.extraJavaOptions",
                        "--add-opens=java.base/java.nio=ALL-UNNAMED " +
                                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.sql.autoBroadcastJoinThreshold", "10485760") // 10MB
                .getOrCreate();

        // Reduce logging level to avoid clutter
        spark.sparkContext().setLogLevel("WARN");

        System.out.println("Hadoop home: " + System.getenv("HADOOP_HOME"));
        System.out.println("winutils exists: " + new File("C:/hadoop/bin/winutils.exe").exists());

        try {
            // Step 1-9: Batch Processing
            Dataset<Row> processedData = processBatchData(spark);

            // Step 10-12: Streaming Processing
            processStreamingData(spark);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }

    private static Dataset<Row> processBatchData(SparkSession spark) {
        // Register UDF for date conversion - using the enhanced version
        spark.udf().register("convertDate",
                DateUtils.createDateConverter(AppConstants.RECEIPT_DATE_FORMAT, AppConstants.OUTPUT_DATE_FORMAT),
                DataTypes.StringType);

        // Step 2: Read and process receipts data with caching
        Dataset<Row> receipts = CSVUtils.readCSV(spark, AppConstants.RECEIPTS_PATH)
                .filter(col(AppConstants.DATE_TIME_COL).like("2022-%"))
                .withColumn(AppConstants.DATE_COL,
                        to_date(callUDF("convertDate", col(AppConstants.DATE_TIME_COL)),
                                AppConstants.OUTPUT_DATE_FORMAT))
                .withColumn(AppConstants.LAT_COL,
                        round(col(AppConstants.LAT_COL), AppConstants.COORDINATE_DECIMALS))
                .withColumn(AppConstants.LNG_COL,
                        round(col(AppConstants.LNG_COL), AppConstants.COORDINATE_DECIMALS))
                .cache();  // Cache the receipts data

        // Force caching and verify
        System.out.println("Receipts count: " + receipts.count());

        // Step 3: Read and process weather data with caching
        Dataset<Row> weather = CSVUtils.readCSV(spark, AppConstants.WEATHER_PATH)
                .filter(col(AppConstants.WTHR_DATE_COL).like("2022%"))
                .withColumn(AppConstants.DATE_COL,
                        to_date(col(AppConstants.WTHR_DATE_COL), AppConstants.WEATHER_DATE_FORMAT))
                .withColumn(AppConstants.LAT_COL,
                        round(col(AppConstants.LAT_COL), AppConstants.COORDINATE_DECIMALS))
                .withColumn(AppConstants.LNG_COL,
                        round(col(AppConstants.LNG_COL), AppConstants.COORDINATE_DECIMALS))
                .cache();  // Cache the weather data

        // Force caching and verify
        System.out.println("Weather count: " + weather.count());

        // Step 4: Join datasets with explicit column references
        Dataset<Row> enrichedData = receipts.join(weather,
                        receipts.col(AppConstants.DATE_COL).equalTo(weather.col(AppConstants.DATE_COL))
                                .and(receipts.col(AppConstants.LAT_COL).equalTo(weather.col(AppConstants.LAT_COL)))
                                .and(receipts.col(AppConstants.LNG_COL).equalTo(weather.col(AppConstants.LNG_COL))),
                        "inner")
                .drop(weather.col(AppConstants.DATE_COL))
                .drop(weather.col(AppConstants.LAT_COL))
                .drop(weather.col(AppConstants.LNG_COL));

        // Step 5: Filter by temperature
        enrichedData = enrichedData.filter(col(AppConstants.AVG_TMPR_COL).gt(AppConstants.MIN_TEMPERATURE));

        // Step 6: Calculate derived fields
        enrichedData = enrichedData
                .withColumn("real_total_cost",
                        col(AppConstants.TOTAL_COST_COL).minus(col(AppConstants.TOTAL_COST_COL).multiply(col(AppConstants.DISCOUNT_COL))))
                .withColumn("order_size",
                        when(col(AppConstants.ITEMS_COUNT_COL).isNull().or(col(AppConstants.ITEMS_COUNT_COL).leq(0)), "Erroneous")
                                .when(col(AppConstants.ITEMS_COUNT_COL).equalTo(1), "Tiny")
                                .when(col(AppConstants.ITEMS_COUNT_COL).between(2, 3), "Small")
                                .when(col(AppConstants.ITEMS_COUNT_COL).between(4, 10), "Medium")
                                .otherwise("Large"));

        // Step 7: Calculate order statistics with explicit column references
        Dataset<Row> orderStats = enrichedData
                .groupBy(
                        col(AppConstants.FRANCHISE_ID_COL),
                        col(AppConstants.DATE_COL),
                        col("order_size")
                )
                .agg(count(col(AppConstants.RECEIPT_ID_COL)).as("order_count"))
                .groupBy(
                        col(AppConstants.FRANCHISE_ID_COL),
                        col(AppConstants.DATE_COL)
                )
                .pivot("order_size", Arrays.asList("Erroneous", "Tiny", "Small", "Medium", "Large"))
                .agg(first("order_count", false))  // Added ignoreNulls parameter
                .na().fill(0)
                .withColumn("most_popular_order_type",
                        when(col("Large").gt(0), "Large")
                                .when(col("Medium").gt(0), "Medium")
                                .when(col("Small").gt(0), "Small")
                                .when(col("Tiny").gt(0), "Tiny")
                                .otherwise("Erroneous"));

        // Final dataset with table aliases to avoid ambiguity
        Dataset<Row> finalData = enrichedData.alias("e").join(
                        orderStats.alias("o"),
                        col("e." + AppConstants.FRANCHISE_ID_COL).equalTo(col("o." + AppConstants.FRANCHISE_ID_COL))
                                .and(col("e." + AppConstants.DATE_COL).equalTo(col("o." + AppConstants.DATE_COL))),
                        "left")
                .drop(col("o." + AppConstants.FRANCHISE_ID_COL))
                .drop(col("o." + AppConstants.DATE_COL));

        // Step 8: Save processed data with partitioning
        finalData.repartition(4)  // Optimized number of partitions
                .write()
                .mode(SaveMode.Overwrite)
                .parquet(AppConstants.PROCESSED_DATA_PATH);

        // Step 9: Prepare CSV files for streaming (8 files)
        CSVUtils.saveAsCSV(finalData, AppConstants.STREAMING_INPUT_PATH, 8);

        // Clean up cached data
        receipts.unpersist();
        weather.unpersist();

        return finalData;
    }

    private static void processStreamingData(SparkSession spark) throws StreamingQueryException, TimeoutException {
        // Define schema for streaming data
        StructType schema = new StructType()
                .add("id", DataTypes.StringType)
                .add("franchise_id", DataTypes.StringType)
                .add("franchise_name", DataTypes.StringType)
                .add("restaurant_franchise_id", DataTypes.StringType)
                .add("country", DataTypes.StringType)
                .add("city", DataTypes.StringType)
                .add("lat", DataTypes.DoubleType)
                .add("lng", DataTypes.DoubleType)
                .add("date", DataTypes.DateType)
                .add("avg_tmpr_c", DataTypes.DoubleType)
                .add("erroneous_orders", DataTypes.IntegerType)
                .add("tiny_orders", DataTypes.IntegerType)
                .add("small_orders", DataTypes.IntegerType)
                .add("medium_orders", DataTypes.IntegerType)
                .add("large_orders", DataTypes.IntegerType)
                .add("most_popular_order_type", DataTypes.StringType)
                .add("receipt_id", DataTypes.StringType)
                .add("total_cost", DataTypes.DoubleType)
                .add("discount", DataTypes.DoubleType)
                .add("real_total_cost", DataTypes.DoubleType)
                .add("items_count", DataTypes.IntegerType)
                .add("order_size", DataTypes.StringType);

        // Read streaming data with optimized settings
        Dataset<Row> streamingData = spark.readStream()
                .schema(schema)
                .option("header", "true")
                .option("maxFilesPerTrigger", 1)
                .option("cleanSource", "delete")  // Clean up processed files
                .csv(AppConstants.STREAMING_INPUT_PATH);

        // Apply temperature-based promotion logic
        Dataset<Row> processedStream = streamingData
                .withColumn("promo_cold_drinks",
                        when(col(AppConstants.AVG_TMPR_COL).gt(AppConstants.PROMO_TEMPERATURE_THRESHOLD), true)
                                .otherwise(false));

        // Write stream to output with optimized settings
        StreamingQuery query = processedStream.writeStream()
                .outputMode("append")
                .format("csv")
                .option("header", "true")
                .option("path", AppConstants.STREAMING_OUTPUT_PATH)
                .option("checkpointLocation", "output/checkpoint")
                .option("truncate", false)
                .start();

        System.out.println("Streaming processing started. Waiting for files in: " + AppConstants.STREAMING_INPUT_PATH);
        query.awaitTermination();
    }
}