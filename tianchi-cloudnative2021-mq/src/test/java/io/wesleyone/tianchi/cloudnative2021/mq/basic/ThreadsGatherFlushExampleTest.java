package io.wesleyone.tianchi.cloudnative2021.mq.basic;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 2000MB 写聚合刷盘
 * single_force cost:16723
 * cyclicBarrier cost:5745
 * phaser cost:6811
 * lock cost:6040
 *
 * @author https://github.com/WesleyOne
 * @create 2021/11/7
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ThreadsGatherFlushExampleTest {

    private static final String ROOT_PATH = "./target";
    private static final String GATHER_PATH = ROOT_PATH+"/gather_flush";
    private final int count = 1024*500;

    @Test
    public void _0_warmup() throws IOException, InterruptedException {
        FileChannel fileChannel = FileChannel.open(Paths.get(GATHER_PATH), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        ExecutorService executor = Executors.newFixedThreadPool(10);
        AtomicLong wrotePosition = new AtomicLong(0);
        int count = 1024*100;
        CountDownLatch countDownLatch = new CountDownLatch(count);
        for(int i=0;i<count;i++){
            executor.execute(()->{
                write(fileChannel, wrotePosition, new byte[4*1024]);
                try {
                    fileChannel.force(false);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                countDownLatch.countDown();
            });
        }
        countDownLatch.await();
        fileChannel.close();
        new File(GATHER_PATH).delete();
    }

    /**
     * 每一条force
     */
    @Test
    public void _1_single_force() throws InterruptedException, IOException {
        FileChannel fileChannel = FileChannel.open(Paths.get(GATHER_PATH), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        ExecutorService executor = Executors.newFixedThreadPool(10);
        AtomicLong wrotePosition = new AtomicLong(0L);
        CountDownLatch countDownLatch = new CountDownLatch(count);
        long start = System.currentTimeMillis();
        for(int i = 0; i < count; ++i) {
            executor.execute(() -> {
                this.write(fileChannel, wrotePosition, new byte[4096]);
                try {
                    fileChannel.force(false);
                } catch (IOException var5) {
                    var5.printStackTrace();
                }

                countDownLatch.countDown();
            });
        }
        countDownLatch.await();
        System.out.println("非聚合刷盘 cost:"+(System.currentTimeMillis()-start));
        fileChannel.close();
        (new File(GATHER_PATH)).delete();
    }

    @Test
    public void _2_cyclicBarrier() throws InterruptedException, IOException {
        FileChannel fileChannel = FileChannel.open(Paths.get(GATHER_PATH), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        ExecutorService executor = Executors.newFixedThreadPool(10);
        AtomicLong wrotePosition = new AtomicLong(0);
        CountDownLatch countDownLatch = new CountDownLatch(count);
        // 10个线程写完，最后一个线程执行刷盘
        CyclicBarrier cyclicBarrier = new CyclicBarrier(10, new Runnable() {
            private final AtomicInteger flushNum = new AtomicInteger();
            @Override
            public void run() {
                try {
                    // System.out.println("force:"+flushNum.getAndIncrement());
                    write(fileChannel, wrotePosition, new byte[40*1024]);
                    fileChannel.force(false);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        long start = System.currentTimeMillis();
        for(int i=0;i<count;i++){
            executor.execute(()->{
                countDownLatch.countDown();
                try {
                    cyclicBarrier.await(1,TimeUnit.SECONDS);
                } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                    e.printStackTrace();
                }
            });
        }
        countDownLatch.await();
        System.out.println("cyclicBarrier cost:"+(System.currentTimeMillis()-start));
        fileChannel.close();
        new File(GATHER_PATH).delete();
    }

    @Test
    public void _2_cyclicBarrier_rwd() throws InterruptedException, IOException {
        RandomAccessFile randomAccessFile = new RandomAccessFile(Paths.get(GATHER_PATH).toFile(), "rwd");
        FileChannel fileChannel = randomAccessFile.getChannel();
        ExecutorService executor = Executors.newFixedThreadPool(10);
        AtomicLong wrotePosition = new AtomicLong(0);
        CountDownLatch countDownLatch = new CountDownLatch(count);
        // 10个线程写完，最后一个线程执行刷盘
        CyclicBarrier cyclicBarrier = new CyclicBarrier(10, new Runnable() {
            private final AtomicInteger flushNum = new AtomicInteger();
            @Override
            public void run() {
                // System.out.println("force:"+flushNum.getAndIncrement());
                write(fileChannel, wrotePosition, new byte[40*1024]);
            }
        });
        long start = System.currentTimeMillis();
        for(int i=0;i<count;i++){
            executor.execute(()->{
                countDownLatch.countDown();
                try {
                    cyclicBarrier.await(1,TimeUnit.SECONDS);
                } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                    e.printStackTrace();
                }
            });
        }
        countDownLatch.await();
        System.out.println("cyclicBarrier_rwd cost:"+(System.currentTimeMillis()-start));
        fileChannel.close();
        new File(GATHER_PATH).delete();
    }

    @Test
    public void _3_phaser() throws InterruptedException, IOException {
        FileChannel fileChannel = FileChannel.open(Paths.get(GATHER_PATH), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        ExecutorService executor = Executors.newFixedThreadPool(10);
        AtomicLong wrotePosition = new AtomicLong(0);
        CountDownLatch countDownLatch = new CountDownLatch(count);
        // 10个线程写完，最后一个线程执行刷盘
        Phaser phaser = new MyPhaser(10, new Runnable() {
            private final AtomicInteger flushNum = new AtomicInteger();
            @Override
            public void run() {
                try {
                    // System.out.println("force:"+flushNum.getAndIncrement());
                    write(fileChannel, wrotePosition, new byte[40*1024]);
                    fileChannel.force(false);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }) ;
        long start = System.currentTimeMillis();
        for(int i=0;i<count;i++){
            executor.execute(()->{
                countDownLatch.countDown();
                try {
                    int ph = phaser.arrive();
                    phaser.awaitAdvanceInterruptibly(ph, 1,TimeUnit.SECONDS);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
        countDownLatch.await();
        System.out.println("phaser cost:"+(System.currentTimeMillis()-start));
        fileChannel.close();
        new File(GATHER_PATH).delete();
    }

    private volatile int awaitCount = 0;

    @Test
    public void _4_lock() throws IOException, InterruptedException {
        FileChannel fileChannel = FileChannel.open(Paths.get(GATHER_PATH), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        ExecutorService executor = Executors.newFixedThreadPool(10);
        AtomicLong wrotePosition = new AtomicLong(0);
        CountDownLatch countDownLatch = new CountDownLatch(count);
        // lock锁
        ReentrantLock lock = new ReentrantLock();
        Condition condition = lock.newCondition();
        AtomicInteger flushNum = new AtomicInteger();
        long start = System.currentTimeMillis();
        for(int i=0;i<count;i++){
            executor.execute(()->{
                lock.lock();
                try {
                    awaitCount++;
                    countDownLatch.countDown();
                    // 10个线程写完，最后一个线程执行刷盘
                    if (awaitCount < 10) {
                        condition.await(3, TimeUnit.SECONDS);
                    } else {
                        // System.out.println("force:"+flushNum.getAndIncrement());
                        write(fileChannel, wrotePosition, new byte[40*1024]);
                        fileChannel.force(true);
                        awaitCount = 0;
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
        System.out.println("lock cost:"+(System.currentTimeMillis()-start));
        fileChannel.close();
        new File(GATHER_PATH).delete();
    }


    private static class MyPhaser extends Phaser {
        private final Runnable runnable;
        public MyPhaser(int parties, Runnable runnable) {
            super(parties);
            this.runnable = runnable;
        }

        /**
         * 重写该方法，实现最后一个到达线程执行的任务
         * @param phase             期数
         * @param registeredParties 注册的总次数
         * @return  true
         */
        @Override
        protected boolean onAdvance(int phase, int registeredParties) {
            runnable.run();
            return super.onAdvance(phase, registeredParties);
        }
    }

    private synchronized void write(FileChannel fileChannel, AtomicLong wrotePosition, byte[] data){
        try {
            fileChannel.write(ByteBuffer.wrap(new byte[4*1024]),wrotePosition.getAndAdd(4*1024));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}