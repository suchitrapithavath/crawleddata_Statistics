package webcrawlerdatasetoperation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;


public class Part2 {
    public static void main(String[] args) throws Exception {
       
        final SparkConf sparkconf = new SparkConf().setAppName("webcrawler").setMaster("local").set("spark.executor.memory", "2g");
        JavaSparkContext sc = new JavaSparkContext(sparkconf);
        //input file
        JavaRDD<String> textFile = sc.textFile("/Users/suchitrap/Desktop/datamining/dataset.tsv");
        //stores list of conferences and location from input file
        PairFunction<String, String, String> keyData =
                new PairFunction<String, String, String>() {
                    public Tuple2<String, String> call(String x) {
                        return new Tuple2(x.split("\t")[2], x.split("\t")[1]);
                    }
                };
        //stores list of conferences per city in ascending order according to name of city
        JavaPairRDD<String, Iterable<String>> counts2 = textFile.mapToPair(keyData).groupByKey().sortByKey();
        counts2.saveAsTextFile("/Users/suchitrap/Desktop");

    }}