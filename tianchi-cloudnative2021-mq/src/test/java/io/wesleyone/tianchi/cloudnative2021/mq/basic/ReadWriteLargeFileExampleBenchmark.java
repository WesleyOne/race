package io.wesleyone.tianchi.cloudnative2021.mq.basic;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

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
 * @author https://github.com/WesleyOne
 * @create 2021/11/7
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class ReadWriteLargeFileExampleBenchmark {

    private static final String ROOT_PATH = "/Users/wesleyone/test";
    private static final String FC_PATH = ROOT_PATH+"/rw_large_file_channel";
    private static final String MMAP_PATH = ROOT_PATH+"/rw_large_file_mmap";

    @Param(value={"100","1000","10240","102400"})
    private int size;

    @Param(value={"1000","10000","20000"})
    private int maxLoop;

    @Benchmark
    public void _FileChannel_write() throws IOException {
        FileChannel fileChannel = FileChannel.open(Paths.get(FC_PATH), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        // 仅公平参照用
        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, (long) size*maxLoop );
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(size);
        for (int i=0;i<maxLoop;i++) {
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
    public void _MappedByteBuffer_write() throws IOException {
        FileChannel fileChannel = FileChannel.open(Paths.get(MMAP_PATH), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, (long) size*maxLoop );
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(size);
        for (int i=0;i<maxLoop;i++) {
            byteBuffer.rewind();
            // 写
            mappedByteBuffer.put(byteBuffer);
        }
        // 刷盘
        mappedByteBuffer.force();
        fileChannel.close();
        new File(MMAP_PATH).delete();
    }

    /**
     * Via the command line:
     *    $ mvn clean install -Dmaven.test.skip=true
     *    $ java -jar target/benchmarks.jar JMHSample_38
     * @param args
     * @throws RunnerException
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ReadWriteLargeFileExampleBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

}