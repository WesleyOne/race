package io.wesleyone.tianchi.cloudnative2021.mq.improve;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 神奇的文件预写
 * 400MB写
 * no prewrite cost:832
 * prewrite-1 cost:653
 * prewrite0 cost:626
 *
 * 800MB写
 * no prewrite cost:1568
 * prewrite-1 cost:1302
 * prewrite0 cost:1229
 *
 * 1200MB写
 * no prewrite cost:2264
 * prewrite-1 cost:3842
 * prewrite0 cost:3699
 *
 * 2000MB写
 * no prewrite cost:2934
 * prewrite-1 cost:4995
 * prewrite0 cost:5019
 *
 * @author https://github.com/WesleyOne
 * @create 2021/11/7
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class FantasticPreWriteExampleTest {

    private static final String ROOT_PATH = "./target";
    private static final String PRE_WRITE_PATH = ROOT_PATH+"/preWrite_flush";
    private volatile int awaitCount = 0;
    private final ByteBuffer writeBuffer = ByteBuffer.allocateDirect(40*1024);
    private final int count = 1024*400;

    @Test
    public void _0_warmup() throws IOException, InterruptedException {
        FileChannel fileChannel = FileChannel.open(Paths.get(PRE_WRITE_PATH), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        ExecutorService executor = Executors.newFixedThreadPool(10);
        AtomicLong wrotePosition = new AtomicLong(0);
        int count = 1024*100;
        CountDownLatch countDownLatch = new CountDownLatch(count);
        for(int i=0;i<count;i++){
            executor.execute(()->{
                write(fileChannel, wrotePosition, new byte[4*1024]);
                try {
                    fileChannel.force(true);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                countDownLatch.countDown();
            });
        }
        countDownLatch.await();
        fileChannel.close();
        new File(PRE_WRITE_PATH).delete();
    }

    /**
     * 预写-1填充
     */
    @Test
    public void _2_pre_write() throws IOException, InterruptedException {
        FileChannel fileChannel = FileChannel.open(Paths.get(PRE_WRITE_PATH), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        // 预写
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(4 * 1024);
        for (int i=0;i<byteBuffer.capacity();i++) {
            byteBuffer.put((byte) -1);
        }
        for (int i=0;i<count;i++) {
            byteBuffer.flip();
            fileChannel.write(byteBuffer);
        }
        fileChannel.force(true);
        fileChannel.position(0);

        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch countDownLatch = new CountDownLatch(count);
        // lock锁
        ReentrantLock lock = new ReentrantLock();
        Condition condition = lock.newCondition();
        long start = System.currentTimeMillis();
        for(int i=0;i<count;i++){
            executor.execute(()->{
                lock.lock();
                try {
                    awaitCount++;
                    writeBuffer.put(ByteBuffer.wrap(new byte[4*1024]));
                    countDownLatch.countDown();
                    // 10个线程写完，最后一个线程执行刷盘
                    if (awaitCount < 10) {
                        condition.await(3, TimeUnit.SECONDS);
                    } else {
                        awaitCount = 0;
                        writeBuffer.flip();
                        fileChannel.write(writeBuffer.slice());
                        fileChannel.force(true);
                        condition.signalAll();
                    }
                } catch (InterruptedException | IOException e) {
                    e.printStackTrace();
                } finally {
                    lock.unlock();
                }
            });
        }
        countDownLatch.await();
        System.out.println("prewrite-1 cost:"+(System.currentTimeMillis()-start));
        fileChannel.close();
        new File(PRE_WRITE_PATH).delete();
    }

    /**
     * 预写0填充
     */
    @Test
    public void _3_pre_write_0() throws IOException, InterruptedException {
        FileChannel fileChannel = FileChannel.open(Paths.get(PRE_WRITE_PATH), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        // 预写
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(4 * 1024);
        for (int i=0;i<byteBuffer.capacity();i++) {
            byteBuffer.put((byte) 0);
        }
        for (int i=0;i<count;i++) {
            byteBuffer.flip();
            fileChannel.write(byteBuffer);
        }
        fileChannel.force(true);
        fileChannel.position(0);

        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch countDownLatch = new CountDownLatch(count);
        // lock锁
        ReentrantLock lock = new ReentrantLock();
        Condition condition = lock.newCondition();
        long start = System.currentTimeMillis();
        for(int i=0;i<count;i++){
            executor.execute(()->{
                lock.lock();
                try {
                    awaitCount++;
                    writeBuffer.put(ByteBuffer.wrap(new byte[4*1024]));
                    countDownLatch.countDown();
                    // 10个线程写完，最后一个线程执行刷盘
                    if (awaitCount < 10) {
                        condition.await(3, TimeUnit.SECONDS);
                    } else {
                        awaitCount = 0;
                        writeBuffer.flip();
                        fileChannel.write(writeBuffer.slice());
                        fileChannel.force(true);
                        condition.signalAll();
                    }
                } catch (InterruptedException | IOException e) {
                    e.printStackTrace();
                } finally {
                    lock.unlock();
                }
            });
        }
        countDownLatch.await();
        System.out.println("prewrite0 cost:"+(System.currentTimeMillis()-start));
        fileChannel.close();
        new File(PRE_WRITE_PATH).delete();
    }

    /**
     * 无预写
     */
    @Test
    public void _1_no_pre_write() throws IOException, InterruptedException {
        FileChannel fileChannel = FileChannel.open(Paths.get(PRE_WRITE_PATH), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch countDownLatch = new CountDownLatch(count);
        // lock锁
        ReentrantLock lock = new ReentrantLock();
        Condition condition = lock.newCondition();
        long start = System.currentTimeMillis();
        for(int i=0;i<count;i++){
            executor.execute(()->{
                lock.lock();
                try {
                    awaitCount++;
                    writeBuffer.put(ByteBuffer.wrap(new byte[4*1024]));
                    countDownLatch.countDown();
                    // 10个线程写完，最后一个线程执行刷盘
                    if (awaitCount < 10) {
                        condition.await(3, TimeUnit.SECONDS);
                    } else {
                        awaitCount = 0;
                        writeBuffer.flip();
                        fileChannel.write(writeBuffer.slice());
                        fileChannel.force(true);
                        condition.signalAll();
                    }
                } catch (InterruptedException | IOException e) {
                    e.printStackTrace();
                } finally {
                    lock.unlock();
                }
            });
        }
        countDownLatch.await();
        System.out.println("no prewrite cost:"+(System.currentTimeMillis()-start));
        fileChannel.close();
        new File(PRE_WRITE_PATH).delete();
    }

    private synchronized void write(FileChannel fileChannel, AtomicLong wrotePosition, byte[] data){
        try {
            fileChannel.write(ByteBuffer.wrap(data),wrotePosition.getAndAdd(data.length));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}