package webcrawlerdatasetoperation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;


public class Part4 {
    public static void main(String[] args) throws Exception {
        final SparkConf conf = new SparkConf().setAppName("Webcrawler").setMaster("local").set("spark.executor.memory", "2g");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> textFile = context.textFile("Users/suchitrap/dataset.tsv");//reading the input file
        // year from conference acronym is extracted and combined with location
        JavaPairRDD<String, Integer> count = textFile
                .flatMap(s -> Arrays.asList(s.split("\t")[2]+" "+s.split("\t")[0].substring((s.split("\t")[0]).length()-4,(s.split("\t")[0]).length())+"  ").iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b)
                .sortByKey();
        count.saveAsTextFile("Users/suchitrap/Q4");//outputfile

    }
}