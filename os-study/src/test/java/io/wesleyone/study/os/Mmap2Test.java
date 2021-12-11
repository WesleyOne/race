package io.wesleyone.study.os;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;

/**
 * @author https://github.com/WesleyOne
 * @create 2021/11/24
 */
public class Mmap2Test {

    public static void main(String[] args) throws IOException, InterruptedException {
        Mmap2Test mmap2Test = new Mmap2Test();
        for (int i=0;i<5;i++) {
            mmap2Test.write();
        }
    }

    public void write() throws IOException, InterruptedException {
        FileChannel fileChannel = FileChannel.open(Paths.get("./data"), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 16 * 1024);
        TimeUnit.SECONDS.sleep(2L);
        mappedByteBuffer.put((byte)1);
        TimeUnit.SECONDS.sleep(2L);
        mappedByteBuffer.force();
        TimeUnit.SECONDS.sleep(2L);
    }

}
