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

        // Initialize Spark Session
        SparkSession spark = SparkSession.builder()
                .config("spark.hadoop.home.dir", "C:/hadoop")
                .config("spark.sql.warehouse.dir", "file:///C:/temp")
                .appName("RestaurantReceiptsWeatherProcessor")
                .master("local[*]")  // Local mode
                .config("spark.driver.extraJavaOptions",
                        "--add-opens=java.base/java.nio=ALL-UNNAMED " +
                                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
                .config("spark.sql.shuffle.partitions", "4")
                .getOrCreate();

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
        // Register UDF for date conversion
        spark.udf().register("convertDateFormat",
                DateUtils.convertDateFormat(AppConstants.RECEIPT_DATE_FORMAT, AppConstants.OUTPUT_DATE_FORMAT),
                DataTypes.StringType);

        // Step 2: Read and process receipts data
        Dataset<Row> receipts = CSVUtils.readCSV(spark, AppConstants.RECEIPTS_PATH)
                .filter(col(AppConstants.DATE_TIME_COL).like("2022-%"))
                .withColumn(AppConstants.DATE_COL,
                        to_date(callUDF("convertDateFormat", col(AppConstants.DATE_TIME_COL)),
                                AppConstants.OUTPUT_DATE_FORMAT))
                .withColumn(AppConstants.LAT_COL,
                        round(col(AppConstants.LAT_COL), AppConstants.COORDINATE_DECIMALS))
                .withColumn(AppConstants.LNG_COL,
                        round(col(AppConstants.LNG_COL), AppConstants.COORDINATE_DECIMALS));

        // Step 3: Read and process weather data
        Dataset<Row> weather = CSVUtils.readCSV(spark, AppConstants.WEATHER_PATH)
                .filter(col(AppConstants.WTHR_DATE_COL).like("2022%"))
                .withColumn(AppConstants.DATE_COL,
                        to_date(col(AppConstants.WTHR_DATE_COL), AppConstants.WEATHER_DATE_FORMAT))
                .withColumn(AppConstants.LAT_COL,
                        round(col(AppConstants.LAT_COL), AppConstants.COORDINATE_DECIMALS))
                .withColumn(AppConstants.LNG_COL,
                        round(col(AppConstants.LNG_COL), AppConstants.COORDINATE_DECIMALS));

        // Step 4: Join datasets
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

        // Step 7: Calculate order statistics
        Dataset<Row> orderStats = enrichedData
                .groupBy(AppConstants.FRANCHISE_ID_COL, AppConstants.DATE_COL, "order_size")
                .agg(count(AppConstants.RECEIPT_ID_COL).as("order_count"))
                .groupBy(AppConstants.FRANCHISE_ID_COL, AppConstants.DATE_COL)
                .pivot("order_size", Arrays.asList("Erroneous", "Tiny", "Small", "Medium", "Large"))
                .agg(first("order_count"))
                .na().fill(0);

        orderStats = orderStats.withColumn("most_popular_order_type",
                when(col("Large").gt(0), "Large")
                        .when(col("Medium").gt(0), "Medium")
                        .when(col("Small").gt(0), "Small")
                        .when(col("Tiny").gt(0), "Tiny")
                        .otherwise("Erroneous"));

        // Final dataset
        Dataset<Row> finalData = enrichedData.join(orderStats,
                        enrichedData.col(AppConstants.FRANCHISE_ID_COL).equalTo(orderStats.col(AppConstants.FRANCHISE_ID_COL))
                                .and(enrichedData.col(AppConstants.DATE_COL).equalTo(orderStats.col(AppConstants.DATE_COL))),
                        "left")
                .drop(orderStats.col(AppConstants.FRANCHISE_ID_COL))
                .drop(orderStats.col(AppConstants.DATE_COL));

        // Step 8: Save processed data
        finalData.write()
                .mode(SaveMode.Overwrite)
                .parquet(AppConstants.PROCESSED_DATA_PATH);

        // Step 9: Prepare CSV files for streaming (8 files)
        CSVUtils.saveAsCSV(finalData, AppConstants.STREAMING_INPUT_PATH, 8);

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

        // Read streaming data
        Dataset<Row> streamingData = spark.readStream()
                .schema(schema)
                .option("header", "true")
                .option("maxFilesPerTrigger", 1) // Process one file at a time
                .csv(AppConstants.STREAMING_INPUT_PATH);

        // Apply temperature-based promotion logic
        Dataset<Row> processedStream = streamingData
                .withColumn("promo_cold_drinks",
                        when(col(AppConstants.AVG_TMPR_COL).gt(AppConstants.PROMO_TEMPERATURE_THRESHOLD), true)
                                .otherwise(false));

        // Write stream to output
        StreamingQuery query = processedStream.writeStream()
                .outputMode("append")
                .format("csv")
                .option("header", "true")
                .option("path", AppConstants.STREAMING_OUTPUT_PATH)
                .option("checkpointLocation", "output/checkpoint")
                .start();

        System.out.println("Streaming processing started. Waiting for files in: " + AppConstants.STREAMING_INPUT_PATH);
        query.awaitTermination();
    }
}