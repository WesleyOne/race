package io.wesleyone.tianchi.cloudnative2021.mq.improve;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * @author https://github.com/WesleyOne
 * @create 2021/11/13
 */
public class UnsafeByteBuffer {

    private static Unsafe unsafe;
    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            unsafe = (Unsafe) f.get(null);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }


    private final long address;
    private final int capacity;
    private int position;
    private int limit;

    public UnsafeByteBuffer(int capacity) {
        this.capacity = capacity;
        this.address = unsafe.allocateMemory(capacity);
        this.position = 0;
        this.limit = capacity;
    }

    public int remaining() {
        return limit - position;
    }

    public void put(byte b) {
        unsafe.putByte(address + position, b);
        position++;
    }

    public byte get() {
        return unsafe.getByte(address + position);
    }

    public int position() {
        return position;
    }

    public void position(int position) {
        this.position = position;
    }

    public void limit(int limit) {
        this.limit = limit;
    }

    public void flip() {
        limit = position;
        position = 0;
    }

    public void clear() {
        position = 0;
        limit = capacity;
    }

}
