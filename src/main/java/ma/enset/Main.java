package ma.enset;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.*;

import static org.apache.spark.sql.functions.*;

public class Main {
    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession.builder()
                .appName("TP Spark Structured Streaming Capteurs HDFS")
                .master("spark://spark-master:7077")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType)
                .add("timestamp", DataTypes.StringType)
                .add("capteur", DataTypes.StringType)
                .add("valeur", DataTypes.DoubleType)
                .add("unite", DataTypes.StringType);

        Dataset<Row> df = spark.readStream()
                .option("header", "true")
                .schema(schema)
                .csv("hdfs://namenode:8020/streaming/capteurs");

        Dataset<Row> mesures = df.withColumn(
                "event_time",
                to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
        );

        Dataset<Row> stats = mesures.groupBy("capteur")
                .agg(
                        avg("valeur").alias("moyenne"),
                        min("valeur").alias("minimum"),
                        max("valeur").alias("maximum"),
                        count("*").alias("nombre_mesures")
                );

        Dataset<Row> anomalies = mesures.filter(col("valeur").gt(50));

        StreamingQuery q1 = stats.writeStream()
                .outputMode("complete")
                .format("console")
                .option("truncate", "false")
                .option("checkpointLocation", "hdfs://namenode:8020/streaming/checkpoints/capteurs/stats")
                .start();

        StreamingQuery q2 = anomalies.writeStream()
                .outputMode("append")
                .format("console")
                .option("truncate", "false")
                .option("checkpointLocation", "hdfs://namenode:8020/streaming/checkpoints/capteurs/anomalies")
                .start();

        q1.awaitTermination();
        q2.awaitTermination();
    }
}