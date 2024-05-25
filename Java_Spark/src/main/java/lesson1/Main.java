package lesson1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello world!");
        SparkConf conf = new SparkConf().setAppName("Lesson1").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> myRdd = sc.parallelize(data);
        myRdd.foreach(num -> System.out.println(num));
    }
}