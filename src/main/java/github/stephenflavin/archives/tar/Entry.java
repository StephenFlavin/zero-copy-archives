package github.stephenflavin.archives.tar;

import java.io.IOException;
import java.nio.file.Path;

import github.stephenflavin.archives.FileMMapPublisher;

public record Entry(Header header, FileMMapPublisher fileMMapPublisher) {

    public static Entry from(Path path) throws IOException {
        return new Entry(github.stephenflavin.archives.tar.Header.from(path), new FileMMapPublisher(path));
    }

}
