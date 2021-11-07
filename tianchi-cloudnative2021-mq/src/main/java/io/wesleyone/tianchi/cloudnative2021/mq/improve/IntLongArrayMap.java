package io.wesleyone.tianchi.cloudnative2021.mq.improve;

/**
 * @author https://github.com/WesleyOne
 * @create 2021/11/7
 */
public class IntLongArrayMap {

    private final int capacity;
    private final long[] values;

    public IntLongArrayMap(int capacity) {
        this.capacity = capacity;
        this.values = new long[capacity];
    }

    public long get(int key) {
        if (key >= capacity) {
            throw new IndexOutOfBoundsException();
        }
        return values[key];
    }

    public long put(int key, long value) {
        values[key] = value;
        return value;
    }
}
