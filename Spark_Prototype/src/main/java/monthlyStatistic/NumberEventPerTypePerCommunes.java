package monthlyStatistic;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import parkInfo.ParkInformationTableCreation;

import static org.apache.spark.sql.functions.*;

public class NumberEventPerTypePerCommunes {

    /**
     * main() is your entry point to the application.
     *
     * @param args
     */
    public static void main(String[] args) {
        NumberEventPerTypePerCommunes app = new NumberEventPerTypePerCommunes();
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

        Dataset<Row> ParkInfo = new ParkInformationTableCreation().create();


        Dataset<Row> events = spark.read().format("csv").option("header", "true")
                .load("../Fake_Data/events/*.csv")
                .filter(col("date_occur_evt").startsWith("2017"))
                .withColumn("year", year(to_date(col("date_occur_evt"), "yyyy-MM-dd HH:mm:SS")))
                .withColumn("month", month(to_date(col("date_occur_evt"), "yyyy-MM-dd HH:mm:SS")))
                .withColumn("week", weekofyear(to_date(col("date_occur_evt"), "yyyy-MM-dd HH:mm:SS")))
                .withColumn("day", dayofweek(to_date(col("date_occur_evt"), "yyyy-MM-dd HH:mm:SS")));


        Dataset<Row> augmentedData = events.join(ParkInfo,
                col("id_equipement").equalTo(col("id_compteur")), "left");

        System.out.println(augmentedData.count()); // On s'attends à 8640 ligne

        Dataset<Row> Stat = augmentedData.groupBy(
                col("year"),
                col("month"),
                        col("type_evt"),
                col("commune_compteur")
                )
                .count();

        Stat.show();
    }

}
