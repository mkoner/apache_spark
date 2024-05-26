package createParallelizeRDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CreateParallelizeRDDTest {
    private SparkConf sparkConf = new SparkConf().setAppName("CreateParallelizeRDD").setMaster("local[*]");

    @Test
    @DisplayName("Create an empty parallelize RDD with no partitions")
    void testCreateEmptyParallelizeRDDNoPartition() {
        try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            JavaRDD<Object> emptyRDD =  sparkContext.emptyRDD();
            System.out.println(emptyRDD);
            System.out.printf("Default number of partitions: %d%n", emptyRDD.getNumPartitions());
        }
    }

    @Test
    @DisplayName("Create an empty parallelize RDD with default partitions")
    void testCreateEmptyParallelizeRDDDefaultPartition() {
        try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            JavaRDD<Object> emptyRDD =  sparkContext.parallelize(List.of());
            System.out.println(emptyRDD);
            System.out.printf("Default number of partitions: %d%n", emptyRDD.getNumPartitions());
        }
    }
    @Test
    @DisplayName("Create a Spark RDD from Java collection using parallelize() method")
    void createSparkRDDUsingParallelizeMethod() {
        try (final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            final List<Integer> data
                    = Stream.iterate(0, n -> n + 5)
                    .limit(20)
                    .collect(Collectors.toList());
            final JavaRDD<Integer> myRdd = sparkContext.parallelize(data);
            System.out.printf("Total elements in RDD: %d%n", myRdd.count());
            System.out.printf("Default number of partitions: %d%n", myRdd.getNumPartitions());

            System.out.println("Elements of RDD: ");
            myRdd.collect().forEach(System.out::println);

            // reduce operations
            final var max = myRdd.reduce(Integer::max);
            final var min = myRdd.reduce(Integer::min);
            final var sum = myRdd.reduce(Integer::sum);
            System.out.printf("MAX~>%d, MIN~>%d, SUM~>%d%n", max, min, sum);
        }
    }

    @Test
    @DisplayName("Create a Spark RDD from Java collection using parallelize() method with given partitions")
    void createSparkRDDUsingParallelizeMethodWithGivenPartitions() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var data
                    = Stream.iterate(0, n -> n + 10)
                    .limit(20)
                    .collect(Collectors.toList());
            final var myRdd = sparkContext.parallelize(data, 25);
            System.out.printf("Total elements in RDD: %d%n", myRdd.count());
            System.out.printf("Given number of partitions: %d%n", myRdd.getNumPartitions());

            System.out.println("Elements of RDD: ");
            myRdd.collect().forEach(System.out::println);

            // reduce operations
            final var max = myRdd.reduce(Integer::max);
            final var min = myRdd.reduce(Integer::min);
            final var sum = myRdd.reduce(Integer::sum);
            System.out.printf("MAX~>%d, MIN~>%d, SUM~>%d%n", max, min, sum);
        }
    }
}
