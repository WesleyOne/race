package io.wesleyone.tianchi.cloudnative2021.mq.basic;

import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * 大文件读写
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
        fileChannel.position(0);
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

}