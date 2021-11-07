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
 * 神奇的4K对齐写（4K倍数）
 *
 * 200MB写
 * 4K cost:438
 * 3K cost:531
 * 3KT4K cost:523
 *
 * 1200MB写
 * 4K cost:2323
 * 3K cost:2845
 * 3KT4K cost:2615
 *
 * 2000MB写
 * 4K cost:3556
 * 3K cost:4805
 * 3KT4K cost:4617
 *
 * @author https://github.com/WesleyOne
 * @create 2021/11/7
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class Fantastic4KBReadWriteExampleTest {

    private static final String ROOT_PATH = "./target";
    private static final String F4K_PATH = ROOT_PATH+"/f4k_flush";
    private volatile int awaitCount = 0;
    private final ByteBuffer writeBuffer = ByteBuffer.allocateDirect(40*1024);
    int size = 200*1024*1024;

    @Test
    public void _0_warmup() throws IOException, InterruptedException {
        FileChannel fileChannel = FileChannel.open(Paths.get(F4K_PATH), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
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
        new File(F4K_PATH).delete();
    }

    @Test
    public void _1_lock_4K() throws IOException, InterruptedException {
        FileChannel fileChannel = FileChannel.open(Paths.get(F4K_PATH), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        ExecutorService executor = Executors.newFixedThreadPool(10);
        int count = size / (4*1024);
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
        System.out.println("4K cost:"+(System.currentTimeMillis()-start));
        fileChannel.close();
        new File(F4K_PATH).delete();
    }


    @Test
    public void _2_lock_3K() throws IOException, InterruptedException {
        FileChannel fileChannel = FileChannel.open(Paths.get(F4K_PATH), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        ExecutorService executor = Executors.newFixedThreadPool(10);
        AtomicLong wrotePosition = new AtomicLong(0);
        int count = size / (3*1024);
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
                    writeBuffer.put(ByteBuffer.wrap(new byte[3*1024]));
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
        System.out.println("3K cost:"+(System.currentTimeMillis()-start));
        fileChannel.close();
        new File(F4K_PATH).delete();
    }

    @Test
    public void _3_lock_3KT4K() throws IOException, InterruptedException {
        FileChannel fileChannel = FileChannel.open(Paths.get(F4K_PATH), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        ExecutorService executor = Executors.newFixedThreadPool(10);
        AtomicLong wrotePosition = new AtomicLong(0);
        int count = size / (3*1024);
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
                    writeBuffer.put(ByteBuffer.wrap(new byte[3*1024]));
                    countDownLatch.countDown();
                    // 10个线程写完，最后一个线程执行刷盘
                    if (awaitCount < 10) {
                        condition.await(3, TimeUnit.SECONDS);
                    } else {
                        awaitCount = 0;
                        // 4k取整
                        int position = writeBuffer.position();
                        int mod = position % (4*1024);
                        if (mod != 0) {
                            writeBuffer.position(position + (4*1024) - mod);
                        }
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
        System.out.println("3KT4K cost:"+(System.currentTimeMillis()-start));
        fileChannel.close();
        new File(F4K_PATH).delete();
    }


    private synchronized void write(FileChannel fileChannel, AtomicLong wrotePosition, byte[] data){
        try {
            fileChannel.write(ByteBuffer.wrap(data),wrotePosition.getAndAdd(data.length));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}