package ru.demmy.largefilesorting.service;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Stack;

@Service
@RequiredArgsConstructor
public class LargeFileSorter {

    @Value("${file.max-lines-per-file}")
    private Long maxLinesPerFile;

    private final FileService fileService;

    /**
     * Sorts large files by splitting to many intermediate small sorted files and merging.
     *
     * @param input         source file path
     * @param output        target file path
     * @param comparator    comparator for sorting
     * @throws IOException
     */
    public void sort(Path input, Path output, Comparator<String> comparator) throws IOException {
        Queue<Path> batches = splitFileToSortedBatches(input, comparator);

        while (batches.size() > 1) {
            Path first = batches.poll();
            Path second = batches.poll();

            Path mergedBatch = merge(first, second, comparator);
            batches.add(mergedBatch);
        }

        if (batches.size() == 1) {
            fileService.replaceFile(batches.poll(), output);
        }
    }

    private Path merge(Path leftFilePath, Path rightFilePath, Comparator<String> comparator) throws IOException {
        if (leftFilePath == null && rightFilePath == null) {
            throw new IOException("Merge failed, no files");
        }
        if (leftFilePath == null || rightFilePath == null) {
            return leftFilePath == null ? rightFilePath : leftFilePath;
        }

        Path mergeResult = fileService.createTempFileAndGetPath();
        try (BufferedWriter writer = fileService.writeFile(mergeResult);
             BufferedReader leftReader = fileService.readFile(leftFilePath);
             BufferedReader rightReader = fileService.readFile(rightFilePath);) {

            String leftLine = null;
            String rightLine = null;
            while (leftReader.ready() || rightReader.ready()) {
                if (leftReader.ready() && leftLine == null) {
                    leftLine = leftReader.readLine() + "\n";
                }
                if (rightReader.ready() && rightLine == null) {
                    rightLine = rightReader.readLine() + "\n";
                }

                if (leftLine == null && rightLine !=null) {
                    writer.write(rightLine);
                    rightLine = null;
                    continue;
                }

                if (rightLine == null && leftLine != null) {
                    writer.write(leftLine);
                    leftLine = null;
                    continue;
                }

                int compareResult = comparator.compare(leftLine, rightLine);

                if (compareResult < 0) {
                    writer.write(leftLine);
                    leftLine = null;
                } else {
                    writer.write(rightLine);
                    rightLine = null;
                }
            }

        }

        fileService.deleteFile(leftFilePath);
        fileService.deleteFile(rightFilePath);
        return mergeResult;
    }


    private Queue<Path> splitFileToSortedBatches(Path input, Comparator<String> comparator) throws IOException {
        Queue<Path> batches = new LinkedList<>();
        try (BufferedReader reader = fileService.readFile(input)) {
            List<String> lines = new ArrayList<>();
            while (reader.ready()) {
                lines.add(reader.readLine());
                if (lines.size() >= maxLinesPerFile) {
                    batches.add(sortAndSaveBatch(lines, comparator));
                    lines = new Stack<>();
                }
            }
            if (!lines.isEmpty()) {
                batches.add(sortAndSaveBatch(lines, comparator));
            }
        }
        return batches;
    }

    private Path sortAndSaveBatch(List<String> batch, Comparator<String> comparator) throws IOException {
        List<String> sortedLines = batch.stream().sorted(comparator).toList();
        Path tempFilePath = fileService.createTempFileAndGetPath();
        fileService.writeCollectionToFile(tempFilePath, sortedLines);
        return tempFilePath;
    }

}
