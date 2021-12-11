package io.wesleyone.study.os;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;

/**
 * @author https://github.com/WesleyOne
 * @create 2021/11/21
 */
public class MmapTest {

    public static void main(String[] args) throws IOException, InterruptedException {
        String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
        System.out.println(pid);
        TimeUnit.SECONDS.sleep(10L);
        System.out.println("start FileChannel.open");
        FileChannel fileChannel = FileChannel.open(Paths.get("./mmapfile"), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 16 * 1024);
        byte[] bytes = new byte[10];
        bytes[0] = 1;
        System.out.println("start mappedByteBuffer.put");
        mappedByteBuffer.put(bytes);
        TimeUnit.SECONDS.sleep(10L);
        System.out.println("start mappedByteBuffer.force");
        mappedByteBuffer.put(bytes);
        mappedByteBuffer.force();
        TimeUnit.SECONDS.sleep(10L);
    }
}
