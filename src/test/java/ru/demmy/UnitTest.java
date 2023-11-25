package ru.demmy;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import ru.demmy.service.FileService;
import ru.demmy.service.LargeFileSorter;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

@Log4j2
@SpringBootTest
public class UnitTest {

    @Value("${file.uniq-separator}")
    private String separator;

    @Autowired
    private FileService fileService;

    @Autowired
    private LargeFileSorter largeFileSorter;

    @AllArgsConstructor
    public static class CustomComparator implements Comparator<String> {

        private final String separator;

        @Override
        public int compare(String o1, String o2) {
            String key1 = o1.split(separator)[0];
            String key2 = o2.split(separator)[0];

            int compareRes = key1.compareTo(key2);
            if (compareRes == 0) {
                return o1.compareTo(o2);
            }
            return compareRes;
        }
    }

    @ParameterizedTest
    @MethodSource(value = "sortTestArgsProvider")
    void sort_test(String unsortedFile, String sortedFile) throws IOException, URISyntaxException {
        Path sourceFile = fileService.createTempFileAndGetPath();
        copyResourceToFile(unsortedFile, sourceFile);

        Path targetFile = fileService.createTempFileAndGetPath();
        largeFileSorter.sort(sourceFile, targetFile, new CustomComparator(separator));

        List<String> resultList = Files.readAllLines(targetFile);
        List<String> expectedLines = readAllResourceLines(sortedFile);

        Assertions.assertLinesMatch(expectedLines, resultList);
    }

    private static Stream<Arguments> sortTestArgsProvider() {
        return Stream.of(
                Arguments.of("examples/sort_test_unsorted_1.txt", "examples/sort_test_sorted_1.txt"),
                Arguments.of("examples/sort_test_unsorted_2.txt", "examples/sort_test_sorted_2.txt"),
                Arguments.of("examples/sort_test_unsorted_3.txt", "examples/sort_test_sorted_3.txt")
        );
    }

    private static void copyResourceToFile(String resourceName, Path path) throws IOException {
        try (InputStream stream = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(resourceName)) {

            Assertions.assertNotNull(stream);
            Files.copy(stream, path, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static List<String> readAllResourceLines(String resourceName) throws IOException, URISyntaxException {
        URL resource = Thread.currentThread().getContextClassLoader().getResource(resourceName);
        Assertions.assertNotNull(resource);
        return Files.readAllLines(Path.of(resource.toURI()));
    }
}


