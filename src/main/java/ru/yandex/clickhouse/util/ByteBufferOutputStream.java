package ru.yandex.clickhouse.util;

import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

public class ByteBufferOutputStream extends OutputStream {

    private ByteBuffer wrappedBuffer;
    private final boolean autoEnlarge;

    public ByteBufferOutputStream(final ByteBuffer wrappedBuffer, final boolean autoEnlarge) {

        this.wrappedBuffer = wrappedBuffer;
        this.autoEnlarge = autoEnlarge;
    }

    public byte[] toByteArray() {
        ByteBuffer buf = toByteBuffer();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        return bytes;
    }

    public ByteBuffer toByteBuffer() {

        final ByteBuffer byteBuffer = wrappedBuffer.duplicate();
        byteBuffer.flip();
        return byteBuffer.asReadOnlyBuffer();
    }

    public void reset() {
        wrappedBuffer.rewind();
    }

    private void growTo(final int minCapacity) {

        final int oldCapacity = wrappedBuffer.capacity();
        int newCapacity = oldCapacity << 1;
        if (newCapacity - minCapacity < 0) {
            newCapacity = minCapacity;
        }
        if (newCapacity < 0) {
            if (minCapacity < 0) { // overflow
                throw new OutOfMemoryError();
            }
            newCapacity = Integer.MAX_VALUE;
        }
        final ByteBuffer oldWrappedBuffer = wrappedBuffer;
        if (wrappedBuffer.isDirect()) {
            wrappedBuffer = ByteBuffer.allocateDirect(newCapacity);
        } else {
            wrappedBuffer = ByteBuffer.allocate(newCapacity);
        }
        oldWrappedBuffer.flip();
        wrappedBuffer.put(oldWrappedBuffer);
    }

    @Override
    public void write(final int b) {

        try {
            wrappedBuffer.put((byte) b);
        } catch (final BufferOverflowException ex) {
            if (autoEnlarge) {
                final int newBufferSize = wrappedBuffer.capacity() * 2;
                growTo(newBufferSize);
                write(b);
            } else {
                throw ex;
            }
        }
    }

    @Override
    public void write(final byte[] bytes) {

        int oldPosition = 0;
        try {
            oldPosition = wrappedBuffer.position();
            wrappedBuffer.put(bytes);
        } catch (final BufferOverflowException ex) {
            if (autoEnlarge) {
                final int newBufferSize
                        = Math.max(wrappedBuffer.capacity() * 2, oldPosition + bytes.length);
                growTo(newBufferSize);
                write(bytes);
            } else {
                throw ex;
            }
        }
    }

    @Override
    public void write(final byte[] bytes, final int off, final int len) {

        int oldPosition = 0;
        try {
            oldPosition = wrappedBuffer.position();
            wrappedBuffer.put(bytes, off, len);
        } catch (final BufferOverflowException ex) {
            if (autoEnlarge) {
                final int newBufferSize
                        = Math.max(wrappedBuffer.capacity() * 2, oldPosition + len);
                growTo(newBufferSize);
                write(bytes, off, len);
            } else {
                throw ex;
            }
        }
    }
}
