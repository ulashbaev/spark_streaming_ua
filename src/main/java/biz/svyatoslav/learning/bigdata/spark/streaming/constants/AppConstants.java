package biz.svyatoslav.learning.bigdata.spark.streaming.constants;

public class AppConstants {
    // File paths
    public static final String RECEIPTS_PATH = "src/main/resources/data/receipt_restaurants.csv";
    public static final String WEATHER_PATH = "src/main/resources/data/weather.csv";
    public static final String PROCESSED_DATA_PATH = "output/processed_data";
    public static final String STREAMING_INPUT_PATH = "src/main/resources/input_files/";
    public static final String STREAMING_OUTPUT_PATH = "src/main/resources/output/streaming_output/";
    // Date formats
    public static final String RECEIPT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String WEATHER_DATE_FORMAT = "yyyyMMdd";
    public static final String OUTPUT_DATE_FORMAT = "yyyy-MM-dd";

    // Column names
    public static final String DATE_TIME_COL = "date_time";
    public static final String WTHR_DATE_COL = "wthr_date";
    public static final String DATE_COL = "date";
    public static final String LAT_COL = "lat";
    public static final String LNG_COL = "lng";
    public static final String AVG_TMPR_COL = "avg_tmpr_c";
    public static final String TOTAL_COST_COL = "total_cost";
    public static final String DISCOUNT_COL = "discount";
    public static final String ITEMS_COUNT_COL = "items_count";
    public static final String FRANCHISE_ID_COL = "restaurant_franchise_id";
    public static final String RECEIPT_ID_COL = "receipt_id";

    // Other constants
    public static final int COORDINATE_DECIMALS = 2;
    public static final double MIN_TEMPERATURE = 0.0;
    public static final double PROMO_TEMPERATURE_THRESHOLD = 25.0;
}