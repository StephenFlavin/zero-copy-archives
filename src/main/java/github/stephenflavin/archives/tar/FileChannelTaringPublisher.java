package github.stephenflavin.archives.tar;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Flow;
import java.util.function.Consumer;

public class FileChannelTaringPublisher implements Flow.Publisher<FileChannelTaringPublisher.WritableByteChannelConsumable> {

    // todo extract constants
    private static final int CHUNK_SIZE = 512;
    private static final ByteBuffer PADDING = ByteBuffer.allocateDirect(CHUNK_SIZE * 2).asReadOnlyBuffer();

    private final Path[] paths;

    public FileChannelTaringPublisher(Path... paths) {
        for (Path path : paths) {
            if (path.toFile().isDirectory()) {
                throw new UnsupportedOperationException("Directory taring is unsupported");
            }
            if (!Files.exists(path)) {
                throw new IllegalArgumentException("File does not exist: " + path);
            }
        }
        this.paths = paths;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super WritableByteChannelConsumable> subscriber) {
        try {
            subscriber.onSubscribe(new TarBallSubscription(subscriber, paths));
        } catch (Throwable ex) {
            subscriber.onError(ex);
        }
    }

    private static class TarBallSubscription implements Flow.Subscription {

        private final Flow.Subscriber<? super WritableByteChannelConsumable> subscriber;
        private final Path[] paths;
        private final int lastIndex;
        private final ArrayBlockingQueue<ByteBuffer> pendingBuffers;

        private int remaining;
        private FileChannel fileChannel;

        private TarBallSubscription(Flow.Subscriber<? super WritableByteChannelConsumable> subscriber,
                                    Path... paths) {
            this.subscriber = subscriber;
            this.paths = paths;
            this.lastIndex = paths.length - 1;
            this.pendingBuffers = new ArrayBlockingQueue<>(3);

            this.remaining = paths.length;
        }

        @Override
        public synchronized void request(long n) {
            if (n < 0) {
                subscriber.onError(new IllegalArgumentException("§3.9: non-positive requests are not allowed!"));
            }
            if (n == 0) {
                return;
            }

            var requested = n;

            if (fileChannel == null && remaining > 0) {
                try {
                    remaining--;
                    var path = paths[transpose((remaining))];
                    var entry = Entry.from(path);

                    pendingBuffers.add(entry.header().getBuffer());

                    long size = Files.size(path);
                    if (size > 0) {
                        fileChannel = FileChannel.open(path, StandardOpenOption.READ);
                    } else {
                        request(n);
                        return;
                    }

                } catch (IOException e) {
                    subscriber.onError(e);
                }
            }

            if (!pendingBuffers.isEmpty()) {
                while (requested > 0 && !pendingBuffers.isEmpty()) {
                    subscriber.onNext(WritableByteChannelConsumable.of(pendingBuffers.poll()));
                    requested--;
                }
            }

            if (requested > 0) {
                if (fileChannel != null) {
                    requested--;
                    try {
                        long size = fileChannel.size();
                        subscriber.onNext(WritableByteChannelConsumable.of(fileChannel));
                        fileChannel = null;
                        int requiredPadding = (int) (CHUNK_SIZE - (size % CHUNK_SIZE));
                        if (requested > 0) {
                            requested--;
                            subscriber.onNext(WritableByteChannelConsumable.of(PADDING.slice(0, requiredPadding)));
                        } else {
                            pendingBuffers.add(PADDING.slice(0, requiredPadding));
                        }
                    } catch (IOException e) {
                        subscriber.onError(e);
                    }
                }
                if (remaining > 0) {
                    request(requested);
                    return;
                }
            }

            if (remaining == 0) {
                if (requested > 0) {
                    subscriber.onNext(WritableByteChannelConsumable.of(PADDING));
                } else {
                    pendingBuffers.add(PADDING);
                }

                if (pendingBuffers.isEmpty()) {
                    subscriber.onComplete();
                }
            }
        }

        private int transpose(int i) {
            return lastIndex - i;
        }

        @Override
        public void cancel() {
            remaining = 0;
            subscriber.onComplete();
        }
    }

    public static class WritableByteChannelConsumable implements Consumer<WritableByteChannel> {

        private final Object writable;
        private final long size;

        public WritableByteChannelConsumable(ByteBuffer bb) {
            size = bb.remaining();
            writable = bb;
        }

        public WritableByteChannelConsumable(FileChannel fc) throws IOException {
            size = fc.size();
            writable = fc;
        }

        private static WritableByteChannelConsumable of(ByteBuffer bb) {
            return new WritableByteChannelConsumable(bb);
        }

        private static WritableByteChannelConsumable of(FileChannel fc) throws IOException {
            return new WritableByteChannelConsumable(fc);
        }

        @Override
        public void accept(WritableByteChannel writableByteChannel) {
            try {
                if (writable instanceof FileChannel fc) {
                    var written = 0L;
                    do {
                        written += fc.transferTo(fc.position(), fc.size() - fc.position(), writableByteChannel);
                        fc.position(written);
                    } while (written != size);
                } else if (writable instanceof ByteBuffer bb) {
                    do {
                        writableByteChannel.write(bb);
                    } while (bb.hasRemaining());
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        public long getSize() {
            return size;
        }
    }
}
