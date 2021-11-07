package io.wesleyone.tianchi.cloudnative2021.mq.basic;

import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;

/**
 * 大文件读写
 *
 * Benchmark                                                        Mode  Cnt    Score     Error  Units
 * ReadWriteLargeFileExampleTest._FileChannel_write_100K_2000       avgt    3   58.293 ±   5.188  ms/op
 * ReadWriteLargeFileExampleTest._FileChannel_write_1K_10000        avgt    3   15.653 ±   9.019  ms/op
 * ReadWriteLargeFileExampleTest._MappedByteBuffer_write_100K_2000  avgt    3  129.992 ± 401.763  ms/op
 * ReadWriteLargeFileExampleTest._MappedByteBuffer_write_1K_10000   avgt    3    7.341 ±  17.133  ms/op
 *
 * @author https://github.com/WesleyOne
 * @create 2021/11/7
 */
@State(value = Scope.Benchmark)
public class ReadWriteLargeFileExampleTest {

    /**
     * FileChannel读写
     * <p>不需要指定文件大小，在写入过程中会自动增大</p>
     * @throws IOException IO异常
     */
    @Test
    public void fileChannel() throws IOException {
        FileChannel fileChannel = FileChannel.open(Paths.get("./target/rw_large_file"), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(4 * 1024);
        // 写
        fileChannel.write(byteBuffer,0);
        // 刷盘
        fileChannel.force(true);
        // 清理复用缓冲区，存放下面读取数据
        byteBuffer.clear();
        // 读
        int read = fileChannel.read(byteBuffer);
        fileChannel.close();
        Assert.assertEquals(4 * 1024, read);
    }

    /**
     * MappedByteBuffer读写
     * <p>需要指定文件大小，超过则报错</p>
     * <p>MappedByteBuffer是DirectByteBuffer父类（有点奇怪哈），可以使用ByteBuffer操作读写</p>
     * @throws IOException IO异常
     */
    @Test
    public void mappedByteBuffer() throws IOException {
        FileChannel fileChannel = FileChannel.open(Paths.get("./target/rw_large_file_mmap"), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 4L * 1024);
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(4 * 1024);
        // 写
        ByteBuffer sliceByteBuffer = mappedByteBuffer.slice();
        sliceByteBuffer.put(byteBuffer);
        // 刷盘
        mappedByteBuffer.force();
        // 读
        sliceByteBuffer = mappedByteBuffer.slice();
        sliceByteBuffer.position(0);
        sliceByteBuffer.limit(4 * 1024);
        ByteBuffer readByteBuffer = sliceByteBuffer.slice();
        fileChannel.close();
    }

    private static final String ROOT_PATH = "/Users/wesleyone/test";
    private static final String FC_PATH = ROOT_PATH+"/rw_large_file_100k";
    private static final String MMAP_PATH = ROOT_PATH+"/rw_large_file_mmap_100k";


    @Benchmark
    @Test
    public void _FileChannel_write_1K_10000() throws IOException {
        FileChannel fileChannel = FileChannel.open(Paths.get(FC_PATH), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, (long) 20 * 1024 * 1024);
        int size =  1024;
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(size);
        for (int i=0;i<10000;i++) {
            byteBuffer.rewind();
            // 写
            fileChannel.write(byteBuffer,size*i);
        }
        // 刷盘
        fileChannel.force(true);
        fileChannel.close();
        new File(FC_PATH).delete();
    }


    @Benchmark
    @Test
    public void _MappedByteBuffer_write_1K_10000() throws IOException {
        FileChannel fileChannel = FileChannel.open(Paths.get(MMAP_PATH), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, (long) 20 * 1024 * 1024);
        int size = 1024;
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(size);
        for (int i=0;i<10000;i++) {
            byteBuffer.rewind();
            // 写
            mappedByteBuffer.put(byteBuffer);
        }
        // 刷盘
        mappedByteBuffer.force();
        fileChannel.close();
        new File(MMAP_PATH).delete();
    }

    @Benchmark
    @Test
    public void _FileChannel_write_100K_2000() throws IOException {
        FileChannel fileChannel = FileChannel.open(Paths.get(FC_PATH), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, (long) 200 * 1024 * 1024);
        int size = 100 * 1024;
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(size);
        for (int i=0;i<2000;i++) {
            byteBuffer.rewind();
            // 写
            fileChannel.write(byteBuffer,size*i);
        }
        // 刷盘
        fileChannel.force(true);
        fileChannel.close();
        new File(FC_PATH).delete();
    }


    @Benchmark
    @Test
    public void _MappedByteBuffer_write_100K_2000() throws IOException {
        FileChannel fileChannel = FileChannel.open(Paths.get(MMAP_PATH), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, (long) 200 * 1024 * 1024);
        int size = 100 * 1024;
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(size);
        for (int i=0;i<2000;i++) {
            byteBuffer.rewind();
            // 写
            mappedByteBuffer.put(byteBuffer);
        }
        // 刷盘
        mappedByteBuffer.force();
        fileChannel.close();
        new File(MMAP_PATH).delete();
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ReadWriteLargeFileExampleTest.class.getSimpleName())
                .forks(1)
                .threads(1)
                .warmupIterations(1).warmupTime(TimeValue.seconds(3))
                .timeUnit(TimeUnit.MILLISECONDS)
                .mode(Mode.AverageTime)
                .measurementIterations(3).measurementTime(TimeValue.seconds(3))
                .build();
        new Runner(opt).run();
    }

}