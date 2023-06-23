package github.stephenflavin.benchmarking;

import static github.stephenflavin.benchmarking.Benchmark.BENCHMARK_FILES;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;

public class ApacheTar {

    private static final Path RESULT_FILE = Path.of("apache-test.tar");

    public static void main(String[] args) {
        var result = Benchmark.test(ApacheTar::tarTestDat,
                1,
                3,
                () -> {
                    try {
                        Files.deleteIfExists(RESULT_FILE);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
        System.out.println(result);
    }

    private static void tarTestDat() {
        try (var fos = Files.newOutputStream(RESULT_FILE);
             var tarArchiveOutputStream = new TarArchiveOutputStream(new BufferedOutputStream(fos, 256 * 1024))) {
            for (Path path : BENCHMARK_FILES) {
                var entry = new TarArchiveEntry(path, path.getFileName().toString(), LinkOption.NOFOLLOW_LINKS);

                long size = Files.size(path);

                entry.setSize(size);

                tarArchiveOutputStream.putArchiveEntry(entry);

                try (var inputStream = new BufferedInputStream(Files.newInputStream(path, StandardOpenOption.READ), (int) Math.min(256 * 1024L, Math.max(size, 1)))) {
                    tarArchiveOutputStream.write(inputStream.readAllBytes());
                }
                tarArchiveOutputStream.closeArchiveEntry();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
