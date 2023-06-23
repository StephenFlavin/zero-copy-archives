package github.stephenflavin.archives.tar;

import static java.nio.charset.StandardCharsets.US_ASCII;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Map;
import java.util.concurrent.TimeUnit;

//struct star_header
//        {                              /* byte offset */
//        char name[100];               /*   0 */
//        char mode[8];                 /* 100 */
//        char uid[8];                  /* 108 */
//        char gid[8];                  /* 116 */
//        char size[12];                /* 124 */
//        char mtime[12];               /* 136 */
//        char chksum[8];               /* 148 */
//        char typeflag;                /* 156 */
//        char linkname[100];           /* 157 */
//        char magic[6];                /* 257 */
//        char version[2];              /* 263 */
//        char uname[32];               /* 265 */
//        char gname[32];               /* 297 */
//        char devmajor[8];             /* 329 */
//        char devminor[8];             /* 337 */
//        char prefix[155];             /* 345 */
//                                      /* 500 */
//        };
//
//    Type flag    Meaning
//    '0' or (ASCII NUL)    Normal file
//    '1'    Hard link
//    '2'    Symbolic link
//    '3'    Character device
//    '4'    Block device
//    '5'    Directory
//    '6'    Named pipe (FIFO)
public final class Header {

    private static final String BLANK = " ";
    private static final byte[] USTAR_MAGIC = "ustar\0".getBytes(US_ASCII);
    private static final byte[] VERSION = "00".getBytes(US_ASCII);
    private static final String[] ZEROS_PADDING = {"", "0", "00", "000", "0000", "00000", "000000", "0000000", "0000000", "00000000",
            "000000000", "0000000000", "00000000000"};
    private static final byte[] ZERO_FIXED_LENGTH_OCTAL = (toFixedLengthOctal(6, 0) + BLANK).getBytes(US_ASCII);
    private static final long MAX_SIZE = (1024 * 1024 * 1024 * 64L) - 1;
    private ByteBuffer buffer;

    public Header(ByteBuffer buffer) {
        if (buffer.capacity() != 512) {
            throw new IllegalArgumentException("Buffer length is not equal to 512 bytes");
        }
        if (buffer.isReadOnly()) {
            this.buffer = ByteBuffer.allocate(512)
                    .put(buffer);
        } else {
            this.buffer = buffer;
        }
    }

    public Header() {
        this.buffer = ByteBuffer.allocate(512);
    }

    public static Header from(Path path) throws IOException {
        var attributes = Files.readAttributes(path, "unix:size,isRegularFile,gid,uid,lastModifiedTime,mode,group,owner");
        return from(path, attributes);
    }

    public static Header from(Path path, Map<String, Object> attributes) {
        if (Boolean.FALSE.equals(attributes.get("isRegularFile"))) {
            throw new UnsupportedOperationException("Only regular files can be tared");
        }

        return new Header()
                .setFileName(path.getFileName().toString())
                .magic(USTAR_MAGIC)
                .version(VERSION)
                .size((long) attributes.get("size"))
                .gid((int) attributes.get("gid"))
                .uid((int) attributes.get("uid"))
                .mtime(((FileTime) attributes.get("lastModifiedTime")).to(TimeUnit.SECONDS))
                .devmajor(ZERO_FIXED_LENGTH_OCTAL)
                .devminor(ZERO_FIXED_LENGTH_OCTAL)
                .setMode((int) attributes.get("mode"))
                .typeFlag((byte) 48) // todo look into support for other types
                .gname(attributes.get("group").toString())
                .uname(attributes.get("owner").toString())
                .build();
    }

    private static String toFixedLengthOctal(int fixedLength, int value) {
        String octalString = Integer.toOctalString(value);
        int requiredPadding = fixedLength - octalString.length();
        return ZEROS_PADDING[requiredPadding] + octalString;
    }

    private static String toFixedLengthOctal(int fixedLength, long value) {
        String octalString = Long.toOctalString(value);
        int requiredPadding = fixedLength - octalString.length();
        return ZEROS_PADDING[requiredPadding] + octalString;
    }

    public ByteBuffer getBuffer() {
        return buffer.asReadOnlyBuffer();
    }

    public String getFileName() {
        byte[] bytes = new byte[100];
        buffer.get(0, bytes);
        return new String(bytes, US_ASCII);
    }

    public Header setFileName(String fileName) {
        byte[] bytes = fileName.getBytes(US_ASCII);
        if (bytes.length > 100) {
            throw new UnsupportedOperationException("File name \"" + fileName + "\"exceeds posix header limit");
        }

        buffer.put(0, bytes);
        return this;
    }

    public String getMode() {
        byte[] bytes = new byte[8];
        buffer.get(100, bytes);
        return new String(bytes, US_ASCII);
    }

    public Header setMode(int mode) {
        buffer.put(100, (Integer.toOctalString(mode) + BLANK).getBytes(US_ASCII));
        return this;
    }

    public String getUid() {
        byte[] bytes = new byte[8];
        buffer.get(108, bytes);
        return new String(bytes, US_ASCII);
    }

    public Header uid(int uid) {
        buffer.put(108, (toFixedLengthOctal(6, uid) + BLANK).getBytes(US_ASCII));
        return this;
    }

    public String getGid() {
        byte[] bytes = new byte[8];
        buffer.get(116, bytes);
        return new String(bytes, US_ASCII);
    }

    public Header gid(int gid) {
        buffer.put(116, (toFixedLengthOctal(6, gid) + BLANK).getBytes(US_ASCII));
        return this;
    }

    public String getSize() {
        byte[] bytes = new byte[12];
        buffer.get(124, bytes);
        return new String(bytes, US_ASCII);
    }

    public Header size(long size) {
        if (size > MAX_SIZE) {
            throw new UnsupportedOperationException("%s is too large at %dbytes. (MAX_FILE_SIZE=%d)".formatted(getFileName(),
                    size,
                    MAX_SIZE));
        }
        buffer.put(124, (toFixedLengthOctal(12, size) + BLANK).getBytes(US_ASCII));
        return this;
    }

    public String getMtime() {
        byte[] bytes = new byte[12];
        buffer.get(136, bytes);
        return new String(bytes, US_ASCII);
    }

    public Header mtime(long mtime) {
        buffer.put(136, (toFixedLengthOctal(12, mtime) + BLANK).getBytes(US_ASCII));
        return this;
    }

    public String getChecksum() {
        byte[] bytes = new byte[12];
        buffer.get(124, bytes);
        return new String(bytes, US_ASCII);
    }

    public Header generateChecksum() {
        // checksum is treated as " " when calculating the checksum
        buffer.put(148, "        ".getBytes(US_ASCII));
        long l = 0;
        for (byte b : buffer.array()) {
            l += b & 0xff;
        }
        String octalString = Long.toOctalString(l);
        buffer.put(148, octalString.getBytes(US_ASCII));
        return this;
    }

    public byte getTypeFlag() {
        return buffer.get(156);
    }

    public Header typeFlag(byte typeflag) {
        buffer.put(156, typeflag);
        return this;
    }

    public String getLinkName() {
        byte[] bytes = new byte[100];
        buffer.get(157, bytes);
        return new String(bytes, US_ASCII);
    }

    public Header linkName(String linkName) {
        byte[] bytes = linkName.getBytes(US_ASCII);
        if (bytes.length > 100) {
            throw new IllegalArgumentException("link name exceeds posix header limit");
        }
        buffer.put(157, bytes);
        return this;
    }

    public String getMagic() {
        byte[] bytes = new byte[6];
        buffer.get(257, bytes);
        return new String(bytes, US_ASCII);
    }

    public Header magic(byte[] magic) {
        if (magic.length > 6) {
            throw new IllegalArgumentException("magic exceeds posix header limit");
        }
        buffer.put(257, magic);
        return this;
    }

    public String getVersion() {
        byte[] bytes = new byte[2];
        buffer.get(263, bytes);
        return new String(bytes, US_ASCII);
    }

    public Header version(byte[] version) {
        if (version.length > 2) {
            throw new IllegalArgumentException("version exceeds posix header limit");
        }
        buffer.put(263, version);
        return this;
    }

    public String getUname() {
        byte[] bytes = new byte[32];
        buffer.get(265, bytes);
        return new String(bytes, US_ASCII);
    }

    public Header uname(String uname) {
        byte[] bytes = uname.getBytes(US_ASCII);
        if (bytes.length > 32) {
            throw new IllegalArgumentException("uname exceeds posix header limit");
        }
        buffer.put(265, bytes);
        return this;
    }

    public String getGname() {
        byte[] bytes = new byte[32];
        buffer.get(297, bytes);
        return new String(bytes, US_ASCII);
    }

    public Header gname(String gname) {
        byte[] bytes = gname.getBytes(US_ASCII);
        if (bytes.length > 32) {
            throw new IllegalArgumentException("gname exceeds posix header limit");
        }
        buffer.put(297, bytes);
        return this;
    }

    public String getDevMajor() {
        byte[] bytes = new byte[8];
        buffer.get(329, bytes);
        return new String(bytes, US_ASCII);
    }

    public Header devmajor(byte[] devmajor) {
        if (devmajor.length > 8) {
            throw new IllegalArgumentException("devmajor exceeds posix header limit");
        }
        buffer.put(329, devmajor);
        return this;
    }

    public String getDevMinor() {
        byte[] bytes = new byte[8];
        buffer.get(337, bytes);
        return new String(bytes, US_ASCII);
    }

    public Header devminor(byte[] devminor) {
        if (devminor.length > 8) {
            throw new IllegalArgumentException("devminor exceeds posix header limit");
        }
        buffer.put(337, devminor);
        return this;
    }

    public String getPrefix() {
        byte[] bytes = new byte[155];
        buffer.get(345, bytes);
        return new String(bytes, US_ASCII);
    }

    public Header prefix(String prefix) {
        byte[] bytes = prefix.getBytes(US_ASCII);
        if (bytes.length > 155) {
            throw new IllegalArgumentException("prefix exceeds posix header limit");
        }
        buffer.put(345, bytes);
        return this;
    }

    public Map<String, Object> asMap() {
        return Map.ofEntries(
                Map.entry("name", getFileName()),
                Map.entry("mode", getMode()),
                Map.entry("uid", getUid()),
                Map.entry("gid", getGid()),
                Map.entry("size", getSize()),
                Map.entry("mtime", getMtime()),
                Map.entry("chksum", getChecksum()),
                Map.entry("typeflag", getTypeFlag()),
                Map.entry("magic", getMagic()),
                Map.entry("version", getVersion()),
                Map.entry("uname", getUname()),
                Map.entry("gname", getGname()),
                Map.entry("devmajor", getDevMajor()),
                Map.entry("devminor", getDevMinor())
        );
    }

    public Header build() {
        generateChecksum();
        buffer = buffer.rewind().asReadOnlyBuffer();
        return this;
    }

}
