package ru.parLab;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Map;


public class SparkJob {
    static final String[] SKIP_VALUES = {"Code", "YEAR"};
    static final String NAME_DELIMITER = "\",";
    static final String DELAY_DELIMITER = ",";
    static final String NULL_STR = "";
    static final String DELIMITER_SLASH = "\"";
    static final Integer AIRPORT_NAME_INDEX = 1;

    static final int AIRPORT_NAME_POSITION = 1;
    static final int PRIMARY_AIRPORT_NAME_POSITION = 11;
    static final int DEST_AIRPORT_ID_DELAY_POSITION  = 14;
    static final int DEST_AIRPORT_ID_NAME_POSITION  = 0;
    static final int DELAY_POSITION = 17;
    static final int CANCELLED_POSITION = 19;
    static final float ZERO = 0f;

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> distAirportDelays = sc.textFile("664600583_T_ONTIME_sample.csv");
        JavaRDD<String> distAirportNames = sc.textFile("L_AIRPORT_ID.cs");

        JavaPairRDD<Integer, String> airportNamesData = distAirportNames
                .filter(s -> !s.contains(SKIP_VALUES[0]))
                .mapToPair(value -> {
                    String[] table = value.split(NAME_DELIMITER);
                    Integer destinationAirportID = Integer.valueOf( table[DEST_AIRPORT_ID_NAME_POSITION]
                            .replaceAll(NULL_STR, DELIMITER_SLASH));

                    return new Tuple2<>(destinationAirportID, table[AIRPORT_NAME_POSITION]);
                });

        JavaPairRDD<Tuple2<Integer, Integer>, SerializableFlight> airportDelaysData =
                distAirportDelays
                .filter(s -> !s.contains(SKIP_VALUES[1]))
                .mapToPair(value -> {
                   String[] table = value.split(DELAY_DELIMITER);
                   int destination_Airport_ID = Integer.parseInt(table[DEST_AIRPORT_ID_DELAY_POSITION]);
                   int primary_Airport_ID = Integer.parseInt(table[PRIMARY_AIRPORT_NAME_POSITION]);
                   float delay = table[DELAY_POSITION].equals(NULL_STR) ? ZERO : Float.parseFloat(table[DELAY_POSITION]);
                   float canceled = Float.parseFloat(table[CANCELLED_POSITION]);
                   return new Tuple2<>(new Tuple2<>(primary_Airport_ID, destination_Airport_ID),
                                new SerializableFlight(destination_Airport_ID, delay, primary_Airport_ID, canceled));
                });

        JavaPairRDD<Tuple2<Integer, Integer>, SerializableFlightCounters> serFlightCounter =
                airportDelaysData.combineByKey(k -> new SerializableFlightCounters(
                    k.getAirportDelay(),
                    k.wasCancelled() == ZERO ? 0 : 1,
                    k.getAirportDelay() > ZERO ? 1 : 0,
                    1),
                    (flightSerCount, p) ->  SerializableFlightCounters.addValue(
                            p.getAirportDelay(),
                            p.getAirportDelay() != ZERO,
                            p.getAirportDelay() != ZERO,
                            flightSerCount),
                    SerializableFlightCounters::split);

        JavaPairRDD<Tuple2<Integer, Integer>, String> serFlightCounterStr =
                serFlightCounter.mapToPair(value -> {
                    value._2();
                    return new Tuple2<>(value._1, SerializableFlightCounters.outAsString(value._2));
                });

        final Broadcast<Map<Integer, String>> airportsBroadcast = sc.broadcast(airportNamesData.collectAsMap());

        JavaRDD<String> output = serFlightCounterStr.map(value ->{
            Map<Integer, String> airNames = airportsBroadcast.value();
            String startAirportName = airNames.get(value._1._1());
            String endAirportName = airNames.get(value._1._2());
            return startAirportName + "  " + endAirportName + "  " + value._2() + "\n";
        });

        output.saveAsTextFile("hdfs://localhost:9000/user/grim/output_lab3");
    }
}
