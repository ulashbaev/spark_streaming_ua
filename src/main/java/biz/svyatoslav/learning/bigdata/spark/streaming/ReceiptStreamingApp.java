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
        // Initialize Spark with Hadoop config for Windows
        System.setProperty("hadoop.home.dir", "C:/hadoop");
        
        SparkSession spark = SparkSession.builder()
                .appName("RestaurantReceiptsProcessor")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///C:/temp")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.driver.extraJavaOptions", 
                    "--add-opens=java.base/java.nio=ALL-UNNAMED " +
                    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
                .getOrCreate();

        try {
            // Batch Processing
            Dataset<Row> processedData = processBatchData(spark);
            
            // Streaming Processing
            processStreamingData(spark);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }

    private static Dataset<Row> processBatchData(SparkSession spark) {
        // Register the safe date converter UDF
        spark.udf().register("safeDateConvert", 
            DateUtils.safeDateConverter(AppConstants.OUTPUT_DATE_FORMAT),
            DataTypes.StringType);

        // Process receipts data with robust date handling
        Dataset<Row> receipts = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(AppConstants.RECEIPTS_PATH)
                .filter(col(AppConstants.DATE_TIME_COL).isNotNull())
                .withColumn("temp_timestamp", 
                    to_timestamp(col(AppConstants.DATE_TIME_COL), AppConstants.RECEIPT_DATE_FORMAT))
                .withColumn(AppConstants.DATE_COL,
                    callUDF("safeDateConvert", col("temp_timestamp")))
                .drop("temp_timestamp")
                .filter(col(AppConstants.DATE_COL).isNotNull())
                .withColumn(AppConstants.LAT_COL, 
                    round(col(AppConstants.LAT_COL), AppConstants.COORDINATE_DECIMALS))
                .withColumn(AppConstants.LNG_COL, 
                    round(col(AppConstants.LNG_COL), AppConstants.COORDINATE_DECIMALS));

        // Process weather data
        Dataset<Row> weather = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(AppConstants.WEATHER_PATH)
                .filter(col(AppConstants.WTHR_DATE_COL).isNotNull())
                .withColumn(AppConstants.DATE_COL, 
                    to_date(col(AppConstants.WTHR_DATE_COL).cast("string"), AppConstants.WEATHER_DATE_FORMAT))
                .filter(col(AppConstants.DATE_COL).isNotNull())
                .withColumn(AppConstants.LAT_COL, 
                    round(col(AppConstants.LAT_COL), AppConstants.COORDINATE_DECIMALS))
                .withColumn(AppConstants.LNG_COL, 
                    round(col(AppConstants.LNG_COL), AppConstants.COORDINATE_DECIMALS));

        // Join and process data
        Dataset<Row> enrichedData = receipts.join(weather,
                receipts.col(AppConstants.DATE_COL).equalTo(weather.col(AppConstants.DATE_COL))
                    .and(receipts.col(AppConstants.LAT_COL).equalTo(weather.col(AppConstants.LAT_COL)))
                    .and(receipts.col(AppConstants.LNG_COL).equalTo(weather.col(AppConstants.LNG_COL))),
                "inner")
                .filter(col(AppConstants.AVG_TMPR_COL).gt(AppConstants.MIN_TEMPERATURE))
                .withColumn("real_total_cost", 
                    col(AppConstants.TOTAL_COST_COL).minus(col(AppConstants.TOTAL_COST_COL).multiply(col(AppConstants.DISCOUNT_COL))))
                .withColumn("order_size",
                    when(col(AppConstants.ITEMS_COUNT_COL).isNull().or(col(AppConstants.ITEMS_COUNT_COL).leq(0)), "Erroneous")
                    .when(col(AppConstants.ITEMS_COUNT_COL).equalTo(1), "Tiny")
                    .when(col(AppConstants.ITEMS_COUNT_COL).between(2, 3), "Small")
                    .when(col(AppConstants.ITEMS_COUNT_COL).between(4, 10), "Medium")
                    .otherwise("Large"));

        // Calculate order statistics
        Dataset<Row> orderStats = enrichedData
                .groupBy(AppConstants.FRANCHISE_ID_COL, AppConstants.DATE_COL, "order_size")
                .agg(count(AppConstants.RECEIPT_ID_COL).as("order_count"))
                .groupBy(AppConstants.FRANCHISE_ID_COL, AppConstants.DATE_COL)
                .pivot("order_size", Arrays.asList("Erroneous", "Tiny", "Small", "Medium", "Large"))
                .agg(first("order_count"))
                .na().fill(0)
                .withColumn("most_popular_order_type",
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

        // Save data
        finalData.write()
                .mode("overwrite")
                .parquet(AppConstants.PROCESSED_DATA_PATH);

        // Prepare CSV files for streaming
        finalData.repartition(8)
                .write()
                .mode("overwrite")
                .option("header", "true")
                .csv(AppConstants.STREAMING_INPUT_PATH);

        return finalData;
    }

    private static void processStreamingData(SparkSession spark) throws StreamingQueryException, TimeoutException {
        StructType schema = new StructType()
                // Include all your fields from the batch processing
                .add("id", DataTypes.StringType)
                .add("date", DataTypes.DateType)
                .add("avg_tmpr_c", DataTypes.DoubleType)
                // Add all other necessary fields
                .add("items_count", DataTypes.IntegerType);

        Dataset<Row> streamingData = spark.readStream()
                .schema(schema)
                .option("header", "true")
                .option("maxFilesPerTrigger", 1)
                .csv(AppConstants.STREAMING_INPUT_PATH);

        Dataset<Row> processedStream = streamingData
                .withColumn("promo_cold_drinks", 
                    col(AppConstants.AVG_TMPR_COL).gt(AppConstants.PROMO_TEMPERATURE_THRESHOLD));

        StreamingQuery query = processedStream.writeStream()
                .outputMode("append")
                .format("csv")
                .option("path", AppConstants.STREAMING_OUTPUT_PATH)
                .option("checkpointLocation", "output/checkpoint")
                .start();

        query.awaitTermination();
    }
}
