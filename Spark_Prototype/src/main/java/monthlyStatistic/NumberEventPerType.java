package monthlyStatistic;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class NumberEventPerType {

    /**
     * main() is your entry point to the application.
     *
     * @param args
     */
    public static void main(String[] args) {
        NumberEventPerType app = new NumberEventPerType();
        app.start();
    }
    /**
     * The processing code.
     */
    public void start() {

        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Joined Informations App")
                .master("local")
                .getOrCreate();


        Dataset<Row> events = spark.read().format("csv").option("header", "true")
                .load("../Fake_Data/events/*.csv")
                .withColumn("year", year(to_date(col("date_occur_evt"), "yyyy-MM-dd HH:mm:SS")))
                .withColumn("month", month(to_date(col("date_occur_evt"), "yyyy-MM-dd HH:mm:SS")))
                .withColumn("week", weekofyear(to_date(col("date_occur_evt"), "yyyy-MM-dd HH:mm:SS")))
                .withColumn("day", dayofweek(to_date(col("date_occur_evt"), "yyyy-MM-dd HH:mm:SS")));


        Dataset<Row> Stat = events.groupBy(
                col("year"),
                col("month"),
                col("type_evt")
                )
                .count();
        Stat.show(100);

    }

}
