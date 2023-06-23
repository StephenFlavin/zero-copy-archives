package github.stephenflavin.archives.tar;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Flow;
import java.util.function.Consumer;

import github.stephenflavin.archives.FileMMapPublisher;
import github.stephenflavin.archives.tar.Entry;

public class TaringPublisher implements Flow.Publisher<ByteBuffer> {

    private static final int CHUNK_SIZE = 512;
    private static final ByteBuffer PADDING = ByteBuffer.allocateDirect(CHUNK_SIZE * 2).asReadOnlyBuffer();

    private final Path[] paths;

    public TaringPublisher(Path... paths) {
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
    public void subscribe(Flow.Subscriber<? super ByteBuffer> subscriber) {
        try {
            subscriber.onSubscribe(new TarBallSubscription(subscriber, paths));
        } catch (Throwable ex) {
            subscriber.onError(ex);
        }
    }

    private static class TarBallSubscription implements Flow.Subscription {

        private final Flow.Subscriber<? super ByteBuffer> subscriber;
        private final Path[] paths;
        private final int lastIndex;
        private final ArrayBlockingQueue<ByteBuffer> pendingBuffers;

        private int remaining;
        private ForwardingFileMMapSubscription fileSubscription;

        private TarBallSubscription(Flow.Subscriber<? super ByteBuffer> subscriber,
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

            if (fileSubscription == null && remaining > 0) {
                try {
                    remaining--;
                    var path = paths[transpose((remaining))];
                    var entry = Entry.from(path);

                    pendingBuffers.add(entry.header().getBuffer());

                    fileSubscription = new ForwardingFileMMapSubscription(subscriber,
                            bb -> {
                                if (bb != null) {
                                    int requiredPadding = CHUNK_SIZE - (bb.capacity() % CHUNK_SIZE);
                                    pendingBuffers.add(PADDING.slice(0, requiredPadding));
                                }
                                fileSubscription = null;
                            });

                    entry.fileMMapPublisher().subscribe(fileSubscription);
                } catch (IOException e) {
                    subscriber.onError(e);
                }
            }

            if (!pendingBuffers.isEmpty()) {
                while (requested > 0 && !pendingBuffers.isEmpty()) {
                    subscriber.onNext(pendingBuffers.poll());
                    requested--;
                }
            }

            if (requested > 0) {
                if (fileSubscription != null) {
                    requested = fileSubscription.request(requested);
                    if (fileSubscription == null && requested > 0) {
                        // recurse until all paths are published or requested is reduced to 0
                        request(requested);
                        return;
                    }
                }
                if (remaining > 0) {
                    request(requested);
                    return;
                }
            }

            if (remaining == 0) {
                if (requested > 0) {
                    subscriber.onNext(PADDING);
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

    private static class ForwardingFileMMapSubscription implements Flow.Subscriber<FileMMapPublisher.FileChunk> {

        private final Flow.Subscriber<? super ByteBuffer> delegate;
        private final Consumer<ByteBuffer> onComplete;
        private Flow.Subscription fileSubscription;
        private ByteBuffer lastChunk;
        private long requestRemaining;

        /**
         * @param delegate   the subscriber to send the {@link FileMMapPublisher.FileChunk} buffer to.
         * @param onComplete a method that is run when {@link Flow.Subscriber#onComplete()} is called which accepts the last published
         *                   buffer.
         */
        private ForwardingFileMMapSubscription(Flow.Subscriber<? super ByteBuffer> delegate,
                                               Consumer<ByteBuffer> onComplete) {
            this.delegate = delegate;
            this.onComplete = onComplete;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            fileSubscription = subscription;
        }

        public long request(long n) {
            requestRemaining = n;
            fileSubscription.request(requestRemaining);
            return requestRemaining;
        }

        @Override
        public void onNext(FileMMapPublisher.FileChunk chunk) {
            requestRemaining -= chunk.numChunks();
            delegate.onNext(chunk.buffer());
            lastChunk = chunk.buffer();
        }

        @Override
        public void onError(Throwable throwable) {
            delegate.onError(throwable);
        }

        @Override
        public void onComplete() {
            onComplete.accept(lastChunk);
        }
    }
}
