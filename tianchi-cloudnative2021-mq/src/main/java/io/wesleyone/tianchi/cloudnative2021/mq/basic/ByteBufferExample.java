package io.wesleyone.tianchi.cloudnative2021.mq.basic;

import java.nio.ByteBuffer;

/**
 * @author http://wesleyone.github.io/
 */
public class ByteBufferExample {

    /**
     * 堆内
     */
    public void heapBuffer() {
        ByteBuffer allocate = ByteBuffer.allocate(1024);
    }

    /**
     * 堆外
     */
    public void offHeapBuffer() {
        ByteBuffer allocate = ByteBuffer.allocateDirect(1024);
    }


}
