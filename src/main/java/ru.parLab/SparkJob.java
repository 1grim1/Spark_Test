package ru.parLab;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;


public class SparkJob {
    static final String SkipValue = "Code";
    static final String Name_Delimiter = "\",";
    static final String NULL_STR = "";
    static final String Delimiter_slash = "\"";
    static final Integer AirportNameIndex = 1;

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> distAirportDelays = sc.textFile("664600583_T_ONTIME_sample.csv");
        JavaRDD<String> distAirportNames = sc.textFile("L_AIRPORT_ID.cs");

        JavaPairRDD<Integer, String> AirportNamesData = distAirportNames
                .filter(s -> !s.contains(SkipValue))
                .mapToPair(value -> {
                    String[] table = value.split(Name_Delimiter);
                    Integer destinationAirportID = Integer.valueOf( table[Integer.parseInt(Name_Delimiter)]
                            .replaceAll(NULL_STR, Delimiter_slash));

                    return new Tuple2<>(destinationAirportID, table[AirportNameIndex]);
                });
    }
}
