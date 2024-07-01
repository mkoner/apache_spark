package mapping;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FlatMapTest {
    @Test
    @DisplayName("Test flatMap() method in Spark RDD")
    void testFlatMapInSparkRDD() {
        final SparkConf conf = new SparkConf().setAppName("RDDFlatMapsTest").setMaster("local[*]");
        try (final JavaSparkContext sc = new JavaSparkContext(conf)) {
            final String testFilePath = Path.of("src", "test", "resources", "magna-carta.txt.gz").toString();
            final JavaRDD<String> lines = sc.textFile(testFilePath);
            System.out.printf("Total lines in file %d%n", lines.count());

            final var mappedLines = lines.map(line -> List.of(line.split("\\s")));
            System.out.printf("Total lines in file %d%n", mappedLines.count());
            assertEquals(lines.count(), mappedLines.count());

            final JavaRDD<String> words = lines.flatMap(line -> List.of(line.split("\\s")).iterator());
            System.out.printf("Total number of words in the file~>%d%n", words.count());

            System.out.println("First few words:");
            words.take(10).forEach(System.out::println);
            System.out.println("--------------------");
        }
    }
}
