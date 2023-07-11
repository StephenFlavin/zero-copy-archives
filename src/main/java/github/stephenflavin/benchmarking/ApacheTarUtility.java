package github.stephenflavin.benchmarking;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static java.lang.System.Logger.Level.INFO;

// temproary for benchmarking
public class ApacheTarUtility {

    private static final System.Logger logger = System.getLogger(ApacheTarUtility.class.toString());

    public static void main(String[] args) throws IOException {
        var archivePath = Path.of(args[0]);
        var recurse = args[1].equals("-r") || args[1].equals("--recurse");
        var skip = recurse ? 2 : 1;

        if (Files.exists(archivePath)) {
            throw new IllegalArgumentException("Archive already exists at " + archivePath);
        }

        var size = new AtomicLong();
        var filesToTar = Arrays.stream(args)
                .skip(skip)
                .map(Path::of)
                .flatMap(path -> {
                    if (path.toFile().isDirectory()) {
                        if (recurse) {
                            return extractFilePathsFromDir(path);
                        }
                        throw new IllegalArgumentException("Provided path \"" + path + "\" is a directory, to extract directory files pass"
                                + " -r or --recurse as the second argument.");
                    }
                    return Stream.of(path);
                })
                .peek(path -> {
                    try {
                        size.addAndGet(Files.size(path));
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .toArray(Path[]::new);

        logger.log(INFO, "Starting to create tarball for {0}bytes at {1}", size.get(), archivePath);
        var started = Clock.systemUTC().millis();
        createTarFile(archivePath, filesToTar);
        var msToCompletion = Clock.systemUTC().millis() - started;
        logger.log(INFO,
                "Completed creating tarball in {0} seconds, achieved {1}bytes/sec",
                msToCompletion / 1000d,
                (size.get() / (double) msToCompletion) * 1000);
        var completedFileSize = Files.size(archivePath);
        System.out.println(ApacheTarUtility.class.getSimpleName()
                + "," + completedFileSize
                + "," + (msToCompletion / 1000d)
                + "," + ((completedFileSize / (double) msToCompletion) * 1000));
    }

    private static void createTarFile(Path archivePath, Path[] filesToTar) {
        try (var fos = Files.newOutputStream(archivePath);
             var tarArchiveOutputStream = new TarArchiveOutputStream(new BufferedOutputStream(fos, 256 * 1024))) {
            for (Path path : filesToTar) {
                var entry = new TarArchiveEntry(path, path.getFileName().toString(), LinkOption.NOFOLLOW_LINKS);

                long size = Files.size(path);

                entry.setSize(size);

                tarArchiveOutputStream.putArchiveEntry(entry);

                if (size > 0) {
                    try (var inputStream = new BufferedInputStream(Files
                            .newInputStream(path, StandardOpenOption.READ), (int) Math.min(256 * 1024L, size))) {
                        tarArchiveOutputStream.write(inputStream.readAllBytes());
                    }
                } else {
                    tarArchiveOutputStream.write(new byte[0]);
                }
                tarArchiveOutputStream.closeArchiveEntry();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Stream<? extends Path> extractFilePathsFromDir(Path path) {
        try {
            return Files.find(path, Integer.MAX_VALUE, (p, bfa) -> bfa.isRegularFile());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
