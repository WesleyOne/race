package io.wesleyone.tianchi.cloudnative2021.mq.basic;

import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.info.GraphLayout;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * 认识ByteBuffer
 *
 * @author https://github.com/WesleyOne
 * @create 2021/11/7
 */
public class ByteBufferExampleTest {

    /**
     * 堆内缓冲区实例对象结构
     * <p>数据保存在堆内数组</p>
     */
    @Test
    public void heapBuffer() {
        ByteBuffer heapByteBuffer = ByteBuffer.allocate(1024);
        System.out.println(ClassLayout.parseInstance(heapByteBuffer).toPrintable());
        /*
         * java.nio.HeapByteBuffer object internals:
         * OFF  SZ      TYPE DESCRIPTION                  VALUE
         *   0   8           (object header: mark)        0x0000000000000001 (non-biasable; age: 0)
         *   8   4           (object header: class)       0x00029788
         *  12   4       int Buffer.mark                  -1
         *  16   8      long Buffer.address               0
         *  24   4       int Buffer.position              0
         *  28   4       int Buffer.limit                 1024
         *  32   4       int Buffer.capacity              1024
         *  36   4       int ByteBuffer.offset            0
         *  40   1   boolean ByteBuffer.isReadOnly        false
         *  41   1   boolean ByteBuffer.bigEndian         true
         *  42   1   boolean ByteBuffer.nativeByteOrder   false
         *  43   1           (alignment/padding gap)
         *  44   4    byte[] ByteBuffer.hb                [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
         * Instance size: 48 bytes
         * Space losses: 1 bytes internal + 0 bytes external = 1 bytes total
         */
        byte[] array = heapByteBuffer.array();
        Assert.assertNotNull(array);
    }

    /**
     * 堆外缓冲区实例对象结构
     */
    @Test
    public void offHeapBuffer() {
        ByteBuffer offHeapByteBuffer = ByteBuffer.allocateDirect(1024);
        System.out.println(ClassLayout.parseInstance(offHeapByteBuffer).toPrintable());
        /*
         * java.nio.DirectByteBuffer object internals:
         * OFF  SZ                     TYPE DESCRIPTION                  VALUE
         *   0   8                          (object header: mark)        0x0000000000000001 (non-biasable; age: 0)
         *   8   4                          (object header: class)       0x0003f9d0
         *  12   4                      int Buffer.mark                  -1
         *  16   8                     long Buffer.address               5369622528
         *  24   4                      int Buffer.position              0
         *  28   4                      int Buffer.limit                 1024
         *  32   4                      int Buffer.capacity              1024
         *  36   4                      int ByteBuffer.offset            0
         *  40   1                  boolean ByteBuffer.isReadOnly        false
         *  41   1                  boolean ByteBuffer.bigEndian         true
         *  42   1                  boolean ByteBuffer.nativeByteOrder   false
         *  43   1                          (alignment/padding gap)
         *  44   4                   byte[] ByteBuffer.hb                null
         *  48   4   java.io.FileDescriptor MappedByteBuffer.fd          null
         *  52   4         java.lang.Object DirectByteBuffer.att         null
         *  56   4         sun.misc.Cleaner DirectByteBuffer.cleaner     (object)
         *  60   4                          (object alignment gap)
         * Instance size: 64 bytes
         * Space losses: 1 bytes internal + 4 bytes external = 5 bytes total
         */
        try {
            offHeapByteBuffer.array();
        } catch (UnsupportedOperationException ignore) {
            return;
        }
        Assert.fail();
    }

    /**
     * 常用操作
     */
    @Test
    public void operation() {
        ByteBuffer heapByteBuffer = ByteBuffer.allocate(1024);
        operation(heapByteBuffer);
        System.out.println("=============");
        ByteBuffer offHeapByteBuffer = ByteBuffer.allocateDirect(1024);
        operation(offHeapByteBuffer);
    }

    /**
     * 比较 堆内 和 堆外
     * https://www.cnkirito.moe/file-io-best-practise/?spm=5176.21852664.0.0.14a2679ekePfG9#%E7%9B%B4%E6%8E%A5%E5%86%85%E5%AD%98-%E5%A0%86%E5%A4%96-VS-%E5%A0%86%E5%86%85%E5%86%85%E5%AD%98
     */
    @Test
    public void campare_heap_and_offheap() throws IOException {
        /*
         * 以上操作时，堆内外缓冲区看起来用途没有啥区别。实际可根据使用场景选择：
         * 1. HeapByteBuffer，占用堆内内存，使用过大空间时容易造成频繁GC甚至FGC；
         *    DirectByteBuffer，数据不占用堆内内存，空间不足时会调用cleaner钩子清理其他堆外缓存，推荐手动主动清理((DirectBuffer) buffer).cleaner().clean();
         */
        ByteBuffer heapByteBuffer = ByteBuffer.allocate(1024);
        System.out.println(GraphLayout.parseInstance(heapByteBuffer).toFootprint());
        /*
         * java.nio.HeapByteBuffer@11028347d footprint:
         *      COUNT       AVG       SUM   DESCRIPTION
         *          1      1040      1040   [B
         *          1        48        48   java.nio.HeapByteBuffer
         *          2                1088   (total)
         */
        ByteBuffer directByteBuffer = ByteBuffer.allocateDirect(1024);
        System.out.println(GraphLayout.parseInstance(directByteBuffer).toFootprint());
         /*
         * java.nio.DirectByteBuffer@23ceabc1d footprint:
         *      COUNT       AVG       SUM   DESCRIPTION
         *          1        32        32   java.lang.ref.ReferenceQueue
         *          1        16        16   java.lang.ref.ReferenceQueue$Lock
         *          3        64       192   java.nio.DirectByteBuffer
         *          1        32        32   java.nio.DirectByteBuffer$Deallocator
         *          2        40        80   sun.misc.Cleaner
         *          1        16        16   sun.misc.Perf
         *          1        24        24   sun.misc.Perf$1
         *         10                 392   (total)
         */

        /*
         * 2. HeapByteBuffer写入FileChannel时，源码是先复制到DirectByteBuffer再写，所以I/O操作的缓存建议直接使用堆外缓存；
         */
        FileChannel fileChannel = FileChannel.open(Paths.get("./target/campare_heap_and_offheap"), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        fileChannel.write(heapByteBuffer);
    }

    /**
     * DirectByteBuffer对象特性引起的hack
     */
    @Test
    public void hack_DirectByteBuffer_Reference() {
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024);
        System.out.println(GraphLayout.parseInstance(byteBuffer).toFootprint());
        /*
         * java.nio.DirectByteBuffer@11028347d footprint:
         *      COUNT       AVG       SUM   DESCRIPTION
         *          1        32        32   java.lang.ref.ReferenceQueue
         *          1        16        16   java.lang.ref.ReferenceQueue$Lock
         *          3        64       192   java.nio.DirectByteBuffer
         *          1        32        32   java.nio.DirectByteBuffer$Deallocator
         *          2        40        80   sun.misc.Cleaner
         *          1        16        16   sun.misc.Perf
         *          1        24        24   sun.misc.Perf$1
         *         10                 392   (total)
         */
        /*
         * DirectByteBuffer所运载数据，并不会在堆内占用空间
         * <p>所以缓存对象时，实质只占用了较少堆内空间。</p>
         * <p>初期评测使用DirectByteBuffer，并且没有做清除和混淆操作，导致评测数据可以全量保存在缓存中，不符合比赛用意</p>
         */
    }

    /**
     * DirectByteBuffer的OOM原理引起的hack
     */
    @Test
    public void hack_DirectByteBuffer_OutOfLimit() {
        ByteBuffer.allocateDirect(1024);
        /*
         * 赛题设置堆外内存最大使用大小为-XX:MaxDirectMemorySize=2G
         * 然而这个配置只防"君子"；
         * 查看 ByteBuffer.allocateDirect(1024) 源码，
         * 其中 tryReserveMemory(size, cap)就是用来统计和校验可用堆外内存，
         * 那么就有会大佬直接调用unsafe.allocateMemory(size); 跳过以上统计校验过程，获得更大的堆外缓存空间
         */
    }



    private void operation(ByteBuffer byteBuffer) {
        System.out.println("        "+ byteBuffer);
        // 写字节
        byteBuffer.put((byte)0);
        System.out.println("put(0): "+ byteBuffer);
        // 翻转（写完后，经过翻转，便于下次读）
        byteBuffer.flip();
        System.out.println("flip(): "+ byteBuffer);
        // 剩余可读或写空间 limit-position
        int remaining = byteBuffer.remaining();
        System.out.println("remain: "+ remaining);
        // 读字节
        byte b = byteBuffer.get();
        System.out.println("get():  "+ byteBuffer);

        // 清理坐标信息（注意数据残留，按照常规API操作是不影响的）
        Buffer clear = byteBuffer.clear();
        System.out.println("clear():  "+ byteBuffer);
        // 创建新缓冲区，共享数据，读写不影响主缓冲区坐标。[position,limit,offset]->[0,limit-position,offset+position]
        ByteBuffer byteBufferSlice = byteBuffer.slice();
        System.out.println("slice:  "+byteBufferSlice);
    }



}