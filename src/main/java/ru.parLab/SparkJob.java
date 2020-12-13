package ru.parLab;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkJob {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> distAirportDelays = sc.textFile("664600583_T_ONTIME_sample.csv");
        JavaRDD<String> distAirportNames = sc.textFile("664600583_T_ONTIME_sample.csv");
    }
}
