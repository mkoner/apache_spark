package lesson1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) {

        try (SparkSession session = SparkSession.builder()
                .appName("Lesson1")
                .master("local[*]").getOrCreate();
             JavaSparkContext sc = new JavaSparkContext(session.sparkContext());) {
            List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
            JavaRDD<Integer> myRdd = sc.parallelize(data);
            myRdd.foreach(num -> System.out.println(num));
            System.out.println("Count: " + myRdd.count());
            System.out.println("Number of partitions: " + myRdd.partitions().size());
            System.out.println("Number of partitions: " + myRdd.getNumPartitions());

            Integer max = myRdd.reduce(Integer::max);
            System.out.println("Max: " + max);
            Integer min = myRdd.reduce(Integer::min);
            System.out.println("Min: " + min);
            Integer sum = myRdd.reduce(Integer::sum);
            System.out.println("Sum: " + sum);

        }
    }
}