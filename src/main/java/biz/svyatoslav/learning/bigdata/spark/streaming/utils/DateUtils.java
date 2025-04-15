package biz.svyatoslav.learning.bigdata.spark.streaming.utils;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtils {
    // More robust UDF that handles both String and Timestamp inputs
    public static UDF1<Object, String> safeDateConverter(String outputFormat) {
        return input -> {
            if (input == null) return null;
            
            try {
                Date date;
                if (input instanceof Timestamp) {
                    date = new Date(((Timestamp) input).getTime());
                } else if (input instanceof String) {
                    // Try multiple common date formats
                    String[] possibleFormats = {
                        "yyyy-MM-dd HH:mm:ss",
                        "yyyyMMdd",
                        "MM/dd/yyyy HH:mm"
                    };
                    
                    for (String format : possibleFormats) {
                        try {
                            date = new SimpleDateFormat(format).parse((String) input);
                            break;
                        } catch (Exception e) {
                            continue;
                        }
                    }
                    if (date == null) return null;
                } else {
                    return null;
                }
                return new SimpleDateFormat(outputFormat).format(date);
            } catch (Exception e) {
                return null;
            }
        };
    }
}
