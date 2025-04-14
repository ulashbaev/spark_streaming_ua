package biz.svyatoslav.learning.bigdata.spark.streaming.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class CSVUtils {
    public static void saveAsCSV(Dataset<Row> data, String path, int numFiles) {
        data.repartition(numFiles)
                .write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv(path);
    }

    public static Dataset<Row> readCSV(SparkSession spark, String path) {
        return spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(path);
    }
}