package mapping;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FilterTest {
    @Test
    @DisplayName("Test filter() method in Spark RDD")
    void testFilterRDD() {
        final SparkConf conf = new SparkConf().setAppName("FilterTest").setMaster("local[*]");
        try (final JavaSparkContext sc = new JavaSparkContext(conf)) {
            final String testFilePath = Path.of("src", "test", "resources", "magna-carta.txt.gz").toString();
            final JavaRDD<String> lines = sc.textFile(testFilePath);
            System.out.printf("Total lines in file %d%n", lines.count());

            final JavaRDD<String> words = lines.flatMap(line -> List.of(line.split("\\s")).iterator());
            System.out.printf("Total number of words in the file~>%d%n", words.count());

            final JavaRDD<String> filteredWords = words.filter(word -> word.length() > 5);
            assertTrue(words.count() > filteredWords.count());
            System.out.println("First filtered words:");
            filteredWords.take(10).forEach(System.out::println);
            System.out.println("--------------------");
        }
    }
}
