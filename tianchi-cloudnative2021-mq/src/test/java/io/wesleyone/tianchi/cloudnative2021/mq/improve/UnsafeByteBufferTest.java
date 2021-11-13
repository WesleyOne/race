package io.wesleyone.tianchi.cloudnative2021.mq.improve;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author https://github.com/WesleyOne
 * @create 2021/11/13
 */
public class UnsafeByteBufferTest {

    @Test
    public void putAndGet() {
        UnsafeByteBuffer unsafeByteBuffer = new UnsafeByteBuffer(1024);
        byte b1 = unsafeByteBuffer.get();
        System.out.println(b1);// 0
        b1 = unsafeByteBuffer.get();
        System.out.println(b1);// 0
        b1 = unsafeByteBuffer.get();
        System.out.println(b1);// 0

        unsafeByteBuffer.position(0);
        unsafeByteBuffer.put((byte)1);

        unsafeByteBuffer.position(0);
        byte b = unsafeByteBuffer.get();
        System.out.println(b);// 1
    }
}