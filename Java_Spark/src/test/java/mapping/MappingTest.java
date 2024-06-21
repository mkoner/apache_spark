package mapping;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MappingTest {

    private final SparkConf sparkConf = new SparkConf().setAppName("RDDMappingTest").setMaster("local[*]");

    private static final List<String> data = new ArrayList<>();
    private final int noOfIterations = 10;

    @BeforeAll
    static void beforeAll() {
        final var dataSize = 100_000;
        for (int i = 0; i < dataSize; i++) {
            data.add(RandomStringUtils.randomAscii(ThreadLocalRandom.current().nextInt(10)));
        }
        assertEquals(dataSize, data.size());
    }

    @Test
    @DisplayName("Test map operation using Spark RDD count() method")
    void testMapOperationUsingSparkRDDCount() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var myRdd = sparkContext.parallelize(data);

            final Instant start = Instant.now();
            for (int i = 0; i < noOfIterations; i++) {
                final var strLengths = myRdd.map(String::length)
                        .reduce(Integer::sum);
//                assertEquals(data.size(), strLengths);
                System.out.println(strLengths);
            }
            final long timeElapsed = (Duration.between(start, Instant.now()).toMillis()) / noOfIterations;
            System.out.printf("[Spark RDD] count() method time taken: %d ms%n%n", timeElapsed);
        }
    }
}
