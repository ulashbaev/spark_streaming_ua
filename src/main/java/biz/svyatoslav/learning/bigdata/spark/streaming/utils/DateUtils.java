package biz.svyatoslav.learning.bigdata.spark.streaming.utils;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtils {
    /**
     * Creates a UDF that handles both String and Timestamp inputs
     * @param inputFormat Expected input format (for String inputs)
     * @param outputFormat Desired output format
     * @return UDF that converts date/timestamp to formatted string
     */
    public static UDF1<Object, String> createDateConverter(String inputFormat, String outputFormat) {
        return input -> {
            if (input == null) {
                return null;
            }

            try {
                Date date;
                if (input instanceof Timestamp) {
                    date = new Date(((Timestamp) input).getTime());
                } else if (input instanceof String) {
                    SimpleDateFormat sdf = new SimpleDateFormat(inputFormat);
                    date = sdf.parse((String) input);
                } else {
                    return null; // Unsupported type
                }

                SimpleDateFormat outFormat = new SimpleDateFormat(outputFormat);
                return outFormat.format(date);
            } catch (Exception e) {
                return null;
            }
        };
    }

    /**
     * UDF specifically for converting Timestamp to formatted string
     * @param outputFormat Desired output format
     * @return UDF that converts Timestamp to string
     */
    public static UDF1<Timestamp, String> timestampToString(String outputFormat) {
        return timestamp -> {
            if (timestamp == null) {
                return null;
            }
            try {
                SimpleDateFormat sdf = new SimpleDateFormat(outputFormat);
                return sdf.format(new Date(timestamp.getTime()));
            } catch (Exception e) {
                return null;
            }
        };
    }
}