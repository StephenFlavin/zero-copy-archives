package github.stephenflavin.benchmarking;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.stream.LongStream;

public class Benchmark {

    static final Path[] BENCHMARK_FILES;

    static {
        try {
            BENCHMARK_FILES = Files.list(Path.of("/Users/stephen/Downloads/"))
                    .filter(path -> !path.toFile().isDirectory())
                    .toArray(Path[]::new);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Result test(Runnable r, int warmUp, int runs, Runnable... optionalCleanupOperations) {
        var results = new long[runs];
        long firstResult = 0;
        for (var i = 0; i < runs + warmUp; i++) {
            long result;
            var start = Instant.now();
            r.run();
            var end = Instant.now();
            result = ChronoUnit.NANOS.between(start, end);
            if (i == 0) {
                firstResult = result;
            }
            if (i >= warmUp) {
                results[i - warmUp] = result;
            }
            for (var cleanupOp : optionalCleanupOperations) {
                cleanupOp.run();
            }
        }
        var stats = LongStream.of(results)
                .summaryStatistics();
        return new Result(firstResult,
                stats.getMin(),
                stats.getMax(),
                stats.getAverage());
    }

    public record Result (long first, long min, long max, double average) {
    }

}
