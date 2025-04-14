package biz.svyatoslav.learning.bigdata.spark.streaming.utils;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtils {
    public static UDF1<String, String> convertDateFormat(String inputFormat, String outputFormat) {
        return dateStr -> {
            if (dateStr == null || dateStr.isEmpty()) return null;
            try {
                SimpleDateFormat inFormat = new SimpleDateFormat(inputFormat);
                SimpleDateFormat outFormat = new SimpleDateFormat(outputFormat);
                Date date = inFormat.parse(dateStr);
                return outFormat.format(date);
            } catch (Exception e) {
                return null;
            }
        };
    }
}