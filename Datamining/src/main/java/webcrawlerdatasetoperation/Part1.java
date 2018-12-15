package webcrawlerdatasetoperation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;


public class Part1 {
    public static void main(String[] args) throws Exception {
        int i;
        final SparkConf sparkconf= new SparkConf().setAppName("Webcrawler").setMaster("local").set("spark.executor.memory","2g");
        JavaSparkContext sc= new JavaSparkContext(sparkconf);
        //input file
        JavaRDD<String> textFile = sc.textFile("/Users/suchitrap/Desktop/datamining/dataset.tsv");
        //it takes location and counts number of conferences
        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split("\t")[2]).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        //it swaps key and value and sorts by key which is number of conferences in descending order
        JavaPairRDD<Integer, String> swappedPair = counts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> item) throws Exception {
                return item.swap();
            }

        })
        .sortByKey(false);
        //swaps back key and value
        JavaPairRDD<String, Integer> swappedPair2 = swappedPair.mapToPair(new PairFunction<Tuple2<Integer,String>, String, Integer>() {
            public Tuple2<String,Integer> call(Tuple2<Integer,String> item) throws Exception {
                return item.swap();

            }

        });
        //stores two outputs in different files
        counts.saveAsTextFile("/Users/suchitrap/Desktop");
        swappedPair2.saveAsTextFile("/Users/suchitrap/Desktop");


    }
}





