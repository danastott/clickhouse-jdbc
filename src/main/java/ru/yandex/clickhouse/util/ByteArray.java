package ru.yandex.clickhouse.util;

public class ByteArray {
    public byte[] bytes;
    public int off;
    public int length;

    public ByteArray() {
        this.bytes = new byte[0];
        reset();
    }

    public ByteArray(byte[] bytes) {
        this.bytes = bytes;
        this.off = 0;
        this.length = bytes.length;
    }

    public void write(byte[] bytes) {
        this.bytes = bytes;
        this.off = 0;
        this.length = bytes.length;
    }

    public void write(byte[] bytes, int off, int length) {
        this.bytes = bytes;
        this.off = off;
        this.length = length;
    }

    public void write(ByteArray ba) {
        this.bytes = ba.bytes;
        this.off = ba.off;
        this.length = ba.length;
    }

    public void writeTo(ByteBufferOutputStream buffer) {
        buffer.write(bytes, off, length);
    }

    public void reset() {
        this.off = 0;
        this.length = 0;
    }
}
