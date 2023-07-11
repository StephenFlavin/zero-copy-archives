package github.stephenflavin.archives.tar;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public class FileChannelsTarUtility {

    private static final System.Logger logger = System.getLogger(FileChannelsTarUtility.class.toString());
    private static ExecutorService executorService = null;

    private static CompletableFuture<Void> createTar(WritableByteChannel writableByteChannel, Path... filesToTar) {
        var taringPublisher = new FileChannelTaringPublisher(filesToTar);

        var future = new AtomicReference<CompletableFuture<Void>>();
        var subscriber = new Flow.Subscriber<FileChannelTaringPublisher.WritableByteChannelConsumable>() {
            private final Semaphore semaphore = new Semaphore(1);

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                logger.log(DEBUG, "Subscribed to TaringPublisher");
                future.set(CompletableFuture.runAsync(() -> subscription.request(Long.MAX_VALUE), executor()));
            }

            @Override
            public void onNext(FileChannelTaringPublisher.WritableByteChannelConsumable item) {
                try {
                    semaphore.acquire();
                    item.accept(writableByteChannel);
                } catch (InterruptedException e) {
                    future.get().completeExceptionally(e);
                    Thread.currentThread().interrupt();
                } finally {
                    semaphore.release();
                }
            }

            @Override
            public void onError(Throwable throwable) {
                logger.log(ERROR, "Exception while processing tar file", throwable);
                future.get().completeExceptionally(throwable);
            }

            @Override
            public void onComplete() {
                try {
                    semaphore.acquire();
                    logger.log(DEBUG, "TaringPublisher completed");
                    future.get().complete(null);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };

        taringPublisher.subscribe(subscriber);
        return future.get();
    }

    private static ExecutorService executor() {
        if (executorService == null) {
            executorService = Executors.newSingleThreadExecutor(r -> new Thread(r, "tar-utility-io-thread"));
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (!executorService.isShutdown()) {
                    logger.log(INFO, "Shutting down tar-utility-io-thread");
                    executorService.shutdown();
                }
            }));
        }
        return executorService;
    }

    public static void main(String[] args) throws FileNotFoundException {
        var archivePath = Path.of(args[0]);
        var recurse = args[1].equals("-r") || args[1].equals("--recurse");
        var skip = recurse ? 2 : 1;

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
            .filter(path -> !path.getFileName().startsWith("."))
                .peek(path -> {
                    try {
                        size.addAndGet(Files.size(path));
                    } catch (IOException e) {
                        throw   new UncheckedIOException(e);
                    }
                })
                .toArray(Path[]::new);

        logger.log(INFO, "Starting to create tarball for {0}bytes at {1}", size.get(), archivePath);
        var started = Clock.systemUTC().millis();

        var randomAccessFile = new RandomAccessFile(archivePath.toString(), "rw");
        var channel = randomAccessFile.getChannel();

        createTar(channel, filesToTar).join();
        var msToCompletion = Clock.systemUTC().millis() - started;
        logger.log(INFO,
                "Completed creating tarball in {0} seconds, achieved {1}bytes/sec",
                msToCompletion / 1000d,
                (size.get() / (double) msToCompletion) * 1000);
        executor().shutdownNow();
    }

    private static Stream<? extends Path> extractFilePathsFromDir(Path path) {
        try {
            return Files.find(path, Integer.MAX_VALUE, (p, bfa) -> bfa.isRegularFile());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
