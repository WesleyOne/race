package io.wesleyone.tianchi.cloudnative2021.mq.basic;

import moe.cnkirito.kdio.DirectIOLib;
import moe.cnkirito.kdio.DirectIOUtils;
import moe.cnkirito.kdio.DirectRandomAccessFile;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
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
import java.util.concurrent.atomic.AtomicLong;

/**
 * 顺序读写比随机读写快
 * <pre>
 * warmup cost:982
 * warmup cost:68
 * sortedWrite cost:783
 * sortedRead cost:68
 * fakeSortWrite cost:1009
 * fakeSortRead cost:67
 * randomWrite cost:1737
 * randomRead cost:264
 * </pre>
 *
 * <p>顺序读比随机读快，顺序写比随机写快 简单介绍 https://www.cnkirito.moe/file-io-best-practise/?#%E9%A1%BA%E5%BA%8F%E8%AF%BB%E6%AF%94%E9%9A%8F%E6%9C%BA%E8%AF%BB%E5%BF%AB%EF%BC%8C%E9%A1%BA%E5%BA%8F%E5%86%99%E6%AF%94%E9%9A%8F%E6%9C%BA%E5%86%99%E5%BF%AB</p>
 * <p>《Linux Page Cache调优在Kafka中的应用》-vivo互联网技术 https://mp.weixin.qq.com/s/MaeXn-kmgLUah78brglFkg</p>
 *
 * @author https://github.com/WesleyOne
 * @create 2021/11/7
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SortedOrRandomReadWriteFileExampleTest {

    private static final String ROOT_PATH = "./target";
    private static final String WARMUP_PATH = ROOT_PATH+"/warmup_file_100k";
    private static final String SORT_PATH = ROOT_PATH+"/sort_file_100k";
    private static final String FAKE_SORT_PATH = ROOT_PATH+"/fakesort_file_100k";
    private static final String RANDOM_PATH1 = ROOT_PATH+"/random_file1_100k";
    private static final String RANDOM_PATH2 = ROOT_PATH+"/random_file2_100k";

    @Test
    public void _0_warmup() throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        FileChannel fileChannel = FileChannel.open(Paths.get(WARMUP_PATH), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        ExecutorService executor = Executors.newFixedThreadPool(10);
        AtomicLong wrotePosition = new AtomicLong(0);
        int count = 1024*100;
        CountDownLatch countDownLatch = new CountDownLatch(count);
        for(int i=0;i<count;i++){
            int finalI = i;
            executor.execute(()->{
                write(fileChannel, wrotePosition, new byte[4*1024]);
                countDownLatch.countDown();
            });
        }
        countDownLatch.await();
        System.out.println("warmup cost:"+(System.currentTimeMillis()-start));
        start = System.currentTimeMillis();
        ByteBuffer readByteBuffer = ByteBuffer.allocateDirect(4 * 1024);
        AtomicLong readPosition = new AtomicLong(0);
        for(int i=0;i<count;i++){
            readByteBuffer.clear();
            fileChannel.read(readByteBuffer, readPosition.getAndAdd(4*1024));
        }
        System.out.println("warmup cost:"+(System.currentTimeMillis()-start));
        fileChannel.close();
        new File(WARMUP_PATH).delete();
    }

    /**
     * 顺序写
     */
    @Test
    public void _1_sortedWrite() throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        FileChannel fileChannel = FileChannel.open(Paths.get(SORT_PATH), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        // 为了比较公平的冗余代码
        FileChannel fileChannel2 = FileChannel.open(Paths.get(RANDOM_PATH2), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        ExecutorService executor = Executors.newFixedThreadPool(10);
        AtomicLong wrotePosition = new AtomicLong(0);
        int count = 1024*100;
        CountDownLatch countDownLatch = new CountDownLatch(count);
        for(int i=0;i<count;i++){
            int finalI = i;
            executor.execute(()->{
                // 为了比较公平的冗余代码
                if (finalI %2==0) {
                } else {
                }
                write(fileChannel, wrotePosition, new byte[4*1024]);
                countDownLatch.countDown();
            });
        }
        countDownLatch.await();
        System.out.println("sortedWrite cost:"+(System.currentTimeMillis()-start));
        start = System.currentTimeMillis();
        ByteBuffer readByteBuffer = ByteBuffer.allocateDirect(4 * 1024);
        AtomicLong readPosition = new AtomicLong(0);
        for(int i=0;i<count;i++){
            readByteBuffer.clear();
            fileChannel.read(readByteBuffer, readPosition.getAndAdd(4*1024));
        }
        System.out.println("sortedRead cost:"+(System.currentTimeMillis()-start));
        fileChannel.close();
        fileChannel2.close();
        new File(SORT_PATH).delete();
        new File(RANDOM_PATH2).delete();
    }


    /**
     * 伪顺序写
     * <p>空洞写问题,导致空洞区域重复加载pagecache</p>
     */
    @Test
    public void _2_fakeSortWrite() throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        FileChannel fileChannel = FileChannel.open(Paths.get(FAKE_SORT_PATH), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        // 为了比较公平的冗余代码
        FileChannel fileChannel2 = FileChannel.open(Paths.get(RANDOM_PATH2), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        ExecutorService executor = Executors.newFixedThreadPool(10);
        AtomicLong wrotePosition = new AtomicLong(0);
        int count = 1024*100;
        CountDownLatch countDownLatch = new CountDownLatch(count);
        for(int i=0;i<count;i++){
            int finalI = i;
            executor.execute(()->{
                try {
                    // 为了比较公平的冗余代码
                    if (finalI %2==0) {
                    } else {
                    }
                    fileChannel.write(ByteBuffer.wrap(new byte[4*1024]),wrotePosition.getAndAdd(4*1024));
                    countDownLatch.countDown();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
        countDownLatch.await();
        System.out.println("fakeSortWrite cost:"+(System.currentTimeMillis()-start));
        start = System.currentTimeMillis();
        ByteBuffer readByteBuffer = ByteBuffer.allocateDirect(4 * 1024);
        AtomicLong readPosition = new AtomicLong(0);
        for(int i=0;i<count;i++){
            readByteBuffer.clear();
            fileChannel.read(readByteBuffer, readPosition.getAndAdd(4*1024));
        }
        System.out.println("fakeSortRead cost:"+(System.currentTimeMillis()-start));
        fileChannel.close();
        fileChannel2.close();
        new File(FAKE_SORT_PATH).delete();
        new File(RANDOM_PATH2).delete();
    }

    /**
     * 随机写
     * <p>譬如数据按照topic储存</p>
     * <p>pagecache失效</p>
     * <p>使用Direct IO优化</p>
     */
    @Test
    public void _3_randomWrite() throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        FileChannel fileChannel = FileChannel.open(Paths.get(RANDOM_PATH1), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        FileChannel fileChannel2 = FileChannel.open(Paths.get(RANDOM_PATH2), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        ExecutorService executor = Executors.newFixedThreadPool(10);
        AtomicLong wrotePosition = new AtomicLong(0);
        int count = 1024*100;
        CountDownLatch countDownLatch = new CountDownLatch(count);
        for(int i=0;i<count;i++){
            int finalI = i;
            executor.execute(()->{
                if (finalI %2==0) {
                    write(fileChannel, wrotePosition, new byte[4*1024]);
                } else {
                    write(fileChannel2, wrotePosition, new byte[4*1024]);
                }
                countDownLatch.countDown();
            });
        }
        countDownLatch.await();
        System.out.println("randomWrite cost:"+(System.currentTimeMillis()-start));
        start = System.currentTimeMillis();
        ByteBuffer readByteBuffer = ByteBuffer.allocateDirect(4 * 1024);
        AtomicLong readPosition = new AtomicLong(0);
        AtomicLong readPosition2 = new AtomicLong(0);
        for(int i=0;i<count;i++){
            readByteBuffer.clear();
            if (i %2==0) {
                fileChannel.read(readByteBuffer, readPosition.getAndAdd(4*1024));
            } else {
                fileChannel2.read(readByteBuffer, readPosition2.getAndAdd(4*1024));
            }
        }
        System.out.println("randomRead cost:"+(System.currentTimeMillis()-start));
        fileChannel.close();
        fileChannel2.close();
        new File(RANDOM_PATH1).delete();
        new File(RANDOM_PATH2).delete();
    }

    @Ignore
    @Test
    public void _4_checkSystemSupportDIO() {
        DirectIOLib libForPath = DirectIOLib.getLibForPath(ROOT_PATH);
        Assert.assertNotNull("系统不支持",libForPath);
    }

    @Ignore
    @Test
    public void _5_randomDIORead() throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        FileChannel fileChannel = FileChannel.open(Paths.get(RANDOM_PATH1), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        FileChannel fileChannel2 = FileChannel.open(Paths.get(RANDOM_PATH2), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        ExecutorService executor = Executors.newFixedThreadPool(10);
        AtomicLong wrotePosition = new AtomicLong(0);
        int count = 1024*100;
        CountDownLatch countDownLatch = new CountDownLatch(count);
        for(int i=0;i<count;i++){
            int finalI = i;
            executor.execute(()->{
                if (finalI %2==0) {
                    write(fileChannel, wrotePosition, new byte[4*1024]);
                } else {
                    write(fileChannel2, wrotePosition, new byte[4*1024]);
                }
                countDownLatch.countDown();
            });
        }
        countDownLatch.await();
        System.out.println("randomWrite cost:"+(System.currentTimeMillis()-start));
        start = System.currentTimeMillis();

        AtomicLong readPosition = new AtomicLong(0);
        AtomicLong readPosition2 = new AtomicLong(0);
        DirectIOLib directIOLib = DirectIOLib.getLibForPath("/");
        ByteBuffer readDIOByteBuffer = DirectIOUtils.allocateForDirectIO(directIOLib, 4 * 1024);
        DirectRandomAccessFile directRandomAccessFile = new DirectRandomAccessFile(new File(RANDOM_PATH1), "r");
        DirectRandomAccessFile directRandomAccessFile2 = new DirectRandomAccessFile(new File(RANDOM_PATH2), "r");
        for(int i=0;i<count;i++){
            readDIOByteBuffer.clear();
            if (i %2==0) {
                directRandomAccessFile.read(readDIOByteBuffer, readPosition.getAndAdd(4*1024));
            } else {
                directRandomAccessFile2.read(readDIOByteBuffer, readPosition2.getAndAdd(4*1024));
            }
        }
        System.out.println("randomDIORead cost:"+(System.currentTimeMillis()-start));
        fileChannel.close();
        fileChannel2.close();
        new File(RANDOM_PATH1).delete();
        new File(RANDOM_PATH2).delete();
    }


    private synchronized void write(FileChannel fileChannel, AtomicLong wrotePosition, byte[] data){
        try {
            fileChannel.write(ByteBuffer.wrap(new byte[4*1024]),wrotePosition.getAndAdd(4*1024));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}