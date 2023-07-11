package github.stephenflavin.benchmarking;

import static github.stephenflavin.benchmarking.Benchmark.BENCHMARK_FILES;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Flow;
import java.util.concurrent.Semaphore;

import github.stephenflavin.archives.tar.ByteBufferTaringPublisher;

public class MyTar {

    private static final String RESULT_FILE = "mytar-test.tar";

    public static void main(String[] args) {
        var result = Benchmark.test(MyTar::createTar,
                1,
                3,
                () -> {
                    try {
                        Files.deleteIfExists(Path.of(RESULT_FILE));
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
        System.out.println(result);
    }

    private static void createTar() {
        ByteBufferTaringPublisher byteBufferTaringPublisher = new ByteBufferTaringPublisher(BENCHMARK_FILES);

        try {
            Flow.Subscriber<ByteBuffer> subscriber = new Flow.Subscriber<>() {
                private final RandomAccessFile randomAccessFile = new RandomAccessFile(RESULT_FILE, "rw");
                private final FileChannel channel = randomAccessFile.getChannel();
                private final Semaphore semaphore = new Semaphore(1);

                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    subscription.request(Integer.MAX_VALUE);
                }

                @Override
                public void onNext(ByteBuffer item) {
                    try {
                        semaphore.acquire();
                        channel.write(item);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } finally {
                        semaphore.release();
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    onComplete();
                }

                @Override
                public void onComplete() {
                    try {
                        semaphore.acquire();
                        randomAccessFile.close();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            };

            byteBufferTaringPublisher.subscribe(subscriber);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
