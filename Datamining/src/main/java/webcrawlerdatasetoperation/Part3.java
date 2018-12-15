package webcrawlerdatasetoperation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;


public class Part3 {
    public static void main(String[] args) throws Exception {
        final SparkConf conf = new SparkConf().setAppName("Webcrawler").setMaster("local").set("spark.executor.memory", "2g");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> textFile = context.textFile("Users/suchitrap/dataset.tsv");// reading the input file
        //stores list of conferences and location from input file
        PairFunction<String, String, String> Data =
                new PairFunction<String, String, String>() {
                    public Tuple2<String, String> call(String x) {
                        return new Tuple2(x.split("\t")[1], x.split("\t")[2]);
                    }
                };
        //according to name of conference,city name is stored
        JavaPairRDD<String, Iterable<String>> count= textFile.mapToPair(Data).groupByKey();
        count.saveAsTextFile("Users/suchitrap/Q3");

    }}