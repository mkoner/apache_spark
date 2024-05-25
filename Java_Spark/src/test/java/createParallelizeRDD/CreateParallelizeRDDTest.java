package createParallelizeRDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

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
}
