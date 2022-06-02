package parkInfo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.to_date;

public class ParkInformationTableCreation {
    /**
     * main() is your entry point to the application.
     *
     * @param args
     */
    public static void main(String[] args) {
        ParkInformationTableCreation app = new ParkInformationTableCreation();
        app.create();
    }

    /**
     * The processing code.
     */
    public Dataset<Row> create() {

        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Joined Informations App")
                .master("local")
                .getOrCreate();


        // Step 1: Ingestion des données du Park
        // ---------

        // Reads a local CSV file with header
        Dataset<Row> dfCommunes = spark.read().format("csv").option("header", "true")
                .load("../Fake_Data/SI_Park_Data/communes.csv");

        // Reads a local CSV file with header
        Dataset<Row> dfDepartments = spark.read().format("csv").option("header", "true")
                .load("../Fake_Data/SI_Park_Data/departements.csv");

        // Reads a local CSV file with header
        Dataset<Row> dfRegions = spark.read().format("csv").option("header", "true")
                .load("../Fake_Data/SI_Park_Data/region.csv");

        // Reads a local CSV file with header
        Dataset<Row> dfPDC = spark.read().format("csv").option("header", "true")
                .load("../Fake_Data/SI_Park_Data/pdc.csv");

        // Reads a local CSV file with header
        Dataset<Row> dfPDK = spark.read().format("csv").option("header", "true")
                .load("../Fake_Data/SI_Park_Data/pdk.csv");

        // Reads a local CSV file with header
        Dataset<Row> dfCompteurs = spark.read().format("csv").option("header", "true")
                .load("../Fake_Data/SI_Park_Data/compteurs.csv");

        // Reads a local CSV file with header
        Dataset<Row> dfConcentrateurs = spark.read().format("csv").option("header", "true")
                .load("../Fake_Data/SI_Park_Data/concentrateurs.csv");

        // Reads a local CSV file with header
        Dataset<Row> dfInstallC = spark.read().format("csv").option("header", "true")
                .load("../Fake_Data/SI_Park_Data/installation_c.csv");
        // Reads a local CSV file with header
        Dataset<Row> dfInstallK = spark.read().format("csv").option("header", "true")
                .load("../Fake_Data/SI_Park_Data/installation_k.csv");

        // Create a dataset to links each county to the department and then the region
        List<String> parkColumnsToDrop = Arrays.asList("id_departement", "id_region", "id_communes");

        Dataset<Row> dfLocation = dfCommunes.join(dfDepartments,
                        dfCommunes.col("id_departement").equalTo(dfDepartments.col("id_departement")),
                        "left")
                .join(dfRegions,
                        dfCommunes.col("id_region").equalTo(dfRegions.col("id_region")))
                .drop(JavaConverters.asScalaBuffer(parkColumnsToDrop));

        // Create the dataset containing all the grouped information per pdc and pdk.
        Dataset<Row> df_Park =
                dfInstallC
                        .join(
                                dfCompteurs,
                                dfInstallC.col("id_compteur").equalTo(dfCompteurs.col("id_compteur")),
                                "left"
                        )
                        .join(
                                dfPDC,
                                dfInstallC.col("id_pdc").equalTo(dfPDC.col("id_pdc")),
                                "left"
                        )
                        .join(
                                dfLocation,
                                dfPDC.col("code_insee_commune").equalTo(dfLocation.col("Code INSEE")),
                                "left"
                        )
                        .drop(dfPDC.col("id_pdc"))
                        .drop(dfCompteurs.col("id_compteur"))
                        .drop(dfCommunes.col("id_commune"))
                        .drop(dfLocation.col("Code INSEE"))
                        .withColumn(
                                "date_deb",
                                to_date(
                                        col("date_deb"),
                                        "yyyyMMdd"
                                )
                        )
                        .withColumn("date_fin",
                                to_date(
                                        col("date_fin"),
                                        "yyyyMMdd"
                                )
                        )
                        .withColumnRenamed("date_deb", "date_deb_compteur")
                        .withColumnRenamed("date_fin", "date_fin_compteur")
                        .withColumnRenamed("constructeur_id", "constructeur_compteur")
                        .withColumnRenamed("code_insee_commune", "insee_compteur")
                        .withColumnRenamed("libelle_commune", "commune_compteur")
                        .withColumnRenamed("libelle_departement", "departement_compteur")
                        .withColumnRenamed("libelle_region", "region_compteur")
                        .join(
                                dfInstallK,
                                dfPDC.col("id_pdk").equalTo(dfInstallK.col("id_pdk")),
                                "left"
                        )
                        .withColumn("date_deb",
                                to_date(col(
                                        "date_deb"),
                                        "yyyyMMdd"
                                )
                        )
                        .withColumn(
                                "date_fin",
                                to_date(
                                        col("date_fin"),
                                        "yyyyMMdd"
                                )
                        )
                        .withColumnRenamed("date_deb", "date_deb_concentrateur")
                        .withColumnRenamed("date_fin", "date_fin_concentrateur")
                        .join(
                                dfPDK,
                                dfInstallK.col("id_pdk").equalTo(dfPDK.col("id_pdk"))
                        )
                        .join(
                                dfConcentrateurs,
                                dfInstallK.col("id_concentrateurs").equalTo(dfConcentrateurs.col("id_consentrateur"))
                        )
                        .withColumnRenamed("constructeur_id", "constructeur_concentrateur")
                        .drop(dfConcentrateurs.col("id_consentrateur"))
                        .drop(dfPDK.col("id_pdk"))
                        .drop(dfPDC.col("id_pdk"))
                        .join(
                                dfLocation,
                                dfPDK.col("code_insee_commune").equalTo(dfLocation.col("Code INSEE")),
                                "left"
                        )
                        .drop(dfLocation.col("Code INSEE"))
                        .drop(dfPDK.col("id_pdk"))
                        .drop(col("insee_compteur"))
                        .drop(dfPDK.col("code_insee_commune"))
                        .withColumnRenamed("libelle_commune", "commune_concentrateur")
                        .withColumnRenamed("libelle_departement", "departement_concentrateur")
                        .withColumnRenamed("libelle_region", "region_concentrateur");

        return df_Park;
    }
}

