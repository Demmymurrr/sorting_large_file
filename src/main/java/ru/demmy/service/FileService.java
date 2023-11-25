package ru.demmy.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.UUID;

@Service
public class FileService {

    @Value("${file.read-buffer}")
    private Integer bufferSize;

    @Value("${file.temp-dir}")
    private String tempDir;

    public Path createTempFileAndGetPath() throws IOException {
        File tempFile = new File(tempDir + "/" + UUID.randomUUID());
        if (!tempFile.createNewFile()) {
            throw new IOException("Error while temp file creation");
        }
        return tempFile.toPath();
    }

    public BufferedReader readFile(Path path) throws IOException {
        File file = getExistedByAbsolutPath(path);
        FileReader fileReader = new FileReader(file);
        return new BufferedReader(fileReader, bufferSize);
    }

    public BufferedWriter writeFile(Path path) throws IOException {
        File file = getExistedByAbsolutPath(path);
        FileWriter fileWriter = new FileWriter(file);
        return new BufferedWriter(fileWriter, bufferSize);
    }

    public void writeCollectionToFile(Path path, Collection<String> collection) throws IOException {
        Files.write(path, collection);
    }

    public void replaceFile(Path source, Path target) throws IOException {
        Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
    }

    public void deleteFile(Path path) throws IOException {
        Files.delete(path);
    }

    private File getExistedByAbsolutPath(Path path) throws IOException {
        File file = path.toFile();
        if (!file.exists() || !file.isFile()) {
            throw new IOException("File not found");
        }
        return file;
    }

}
