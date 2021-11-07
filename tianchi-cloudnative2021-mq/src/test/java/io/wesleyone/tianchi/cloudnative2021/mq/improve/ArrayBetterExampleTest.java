package io.wesleyone.tianchi.cloudnative2021.mq.improve;

import io.netty.util.collection.IntObjectHashMap;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.openjdk.jol.info.GraphLayout;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * Map优化
 *
 * 50000条数据：
 * Benchmark                                                        Mode  Cnt  Score   Error  Units
 * ArrayBetterExampleTest.hashMap                                   avgt    3  0.324 ± 0.117  ms/op
 * ArrayBetterExampleTest.hashMap:_0_hashMap_put                    avgt    3  0.502 ± 0.228  ms/op
 * ArrayBetterExampleTest.hashMap:_1_hashMap_get                    avgt    3  0.146 ± 0.014  ms/op
 * ArrayBetterExampleTest.intObjectHashMap                          avgt    3  0.113 ± 0.005  ms/op
 * ArrayBetterExampleTest.intObjectHashMap:_2_intObjectHashMap_put  avgt    3  0.141 ± 0.006  ms/op
 * ArrayBetterExampleTest.intObjectHashMap:_3_intObjectHashMap_get  avgt    3  0.086 ± 0.007  ms/op
 * ArrayBetterExampleTest.intLongArrayMap                           avgt    3  0.028 ± 0.004  ms/op
 * ArrayBetterExampleTest.intLongArrayMap:_4_intLongArrayMap_put    avgt    3  0.016 ± 0.005  ms/op
 * ArrayBetterExampleTest.intLongArrayMap:_5_intLongArrayMap_get    avgt    3  0.039 ± 0.003  ms/op
 *
 * 参考《从Java视角理解CPU缓存(CPU Cache)》https://www.iteye.com/blog/coderplay-1485760
 * @author https://github.com/WesleyOne
 * @create 2021/11/7
 */
@State(value = Scope.Group)
public class ArrayBetterExampleTest {

    private static final int COUNT = 50000;
    private final HashMap<Integer,Long> hashMap = new HashMap(COUNT,1.0F);
    private final IntObjectHashMap<Long> intObjectHashMap = new IntObjectHashMap<Long>(COUNT,1.0F);
    private final IntLongArrayMap intLongArrayMap = new IntLongArrayMap(COUNT);

    @Group("hashMap")
    @Benchmark
    public void _0_hashMap_put() {
        for (int i=0;i<COUNT;i++) {
            hashMap.put(i, (long) (Integer.MAX_VALUE+i));
        }
    }

    @Group("hashMap")
    @Benchmark
    public void _1_hashMap_get() {
        for (int i=0;i<COUNT;i++) {
            Long result = hashMap.get(i);
        }
    }

    @Group("intObjectHashMap")
    @Benchmark
    public void _2_intObjectHashMap_put() {
        for (int i=0;i<COUNT;i++) {
            intObjectHashMap.put(i, new Long(Integer.MAX_VALUE+i));
        }
    }

    @Group("intObjectHashMap")
    @Benchmark
    public void _3_intObjectHashMap_get() {
        for (int i=0;i<COUNT;i++) {
            Long result = intObjectHashMap.get(i);
        }
    }

    @Group("intLongArrayMap")
    @Benchmark
    public void _4_intLongArrayMap_put() {
        for (int i=0;i<COUNT;i++) {
            intLongArrayMap.put(i, Integer.MAX_VALUE+i);
        }
    }

    @Group("intLongArrayMap")
    @Benchmark
    public void _5_intLongArrayMap_get() {
        for (int i=0;i<COUNT;i++) {
            Long result = intLongArrayMap.get(i);
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ArrayBetterExampleTest.class.getSimpleName())
                .forks(1)
                .threads(1)
                .warmupIterations(3).warmupTime(TimeValue.seconds(1))
                .timeUnit(TimeUnit.MILLISECONDS)
                .mode(Mode.AverageTime)
                .measurementIterations(3).measurementTime(TimeValue.seconds(1))
                .build();
        new Runner(opt).run();
    }

    @Test
    public void print_hashMap() {
        System.out.println(GraphLayout.parseInstance(hashMap).toFootprint());
        /*
         * java.util.HashMap@75828a0fd footprint:
         *      COUNT       AVG       SUM   DESCRIPTION
         *          1        48        48   java.util.HashMap
         *          1                  48   (total)
         */
        _0_hashMap_put();
        System.out.println(GraphLayout.parseInstance(hashMap).toFootprint());
        /*
         * java.util.HashMap@75828a0fd footprint:
         *      COUNT       AVG       SUM   DESCRIPTION
         *          1    262160    262160   [Ljava.util.HashMap$Node;
         *      50000        16    800000   java.lang.Integer
         *      50000        24   1200000   java.lang.Long
         *          1        48        48   java.util.HashMap
         *      50000        32   1600000   java.util.HashMap$Node
         *     150002             3862208   (total)
         */
    }

    @Test
    public void print_intObjectHashMap() {
        System.out.println(GraphLayout.parseInstance(intObjectHashMap).toFootprint());
        /**
         * io.netty.util.collection.IntObjectHashMap@75828a0fd footprint:
         *      COUNT       AVG       SUM   DESCRIPTION
         *          1    262160    262160   [I
         *          1    262160    262160   [Ljava.lang.Object;
         *          1        48        48   io.netty.util.collection.IntObjectHashMap
         *          1        16        16   io.netty.util.collection.IntObjectHashMap$1
         *          1        16        16   io.netty.util.collection.IntObjectHashMap$EntrySet
         *          1        16        16   io.netty.util.collection.IntObjectHashMap$KeySet
         *          6              524416   (total)
         */
        _2_intObjectHashMap_put();
        System.out.println(GraphLayout.parseInstance(intObjectHashMap).toFootprint());
        /**
         * io.netty.util.collection.IntObjectHashMap@75828a0fd footprint:
         *      COUNT       AVG       SUM   DESCRIPTION
         *          1    262160    262160   [I
         *          1    262160    262160   [Ljava.lang.Object;
         *          1        48        48   io.netty.util.collection.IntObjectHashMap
         *          1        16        16   io.netty.util.collection.IntObjectHashMap$1
         *          1        16        16   io.netty.util.collection.IntObjectHashMap$EntrySet
         *          1        16        16   io.netty.util.collection.IntObjectHashMap$KeySet
         *      50000        24   1200000   java.lang.Long
         *      50006             1724416   (total)
         */
    }

    @Test
    public void print_intLongArrayMap() {
        System.out.println(GraphLayout.parseInstance(intLongArrayMap).toFootprint());
        /**
         * io.wesleyone.tianchi.cloudnative2021.mq.improve.IntLongArrayMap@75828a0fd footprint:
         *      COUNT       AVG       SUM   DESCRIPTION
         *          1    400016    400016   [J
         *          1        24        24   io.wesleyone.tianchi.cloudnative2021.mq.improve.IntLongArrayMap
         *          2              400040   (total)
         */
        _4_intLongArrayMap_put();
        System.out.println(GraphLayout.parseInstance(intLongArrayMap).toFootprint());
        /**
         * io.wesleyone.tianchi.cloudnative2021.mq.improve.IntLongArrayMap@75828a0fd footprint:
         *      COUNT       AVG       SUM   DESCRIPTION
         *          1    400016    400016   [J
         *          1        24        24   io.wesleyone.tianchi.cloudnative2021.mq.improve.IntLongArrayMap
         *          2              400040   (total)
         */
    }
}