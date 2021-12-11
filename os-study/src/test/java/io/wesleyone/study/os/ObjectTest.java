package io.wesleyone.study.os;


import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

/**
 * @author https://github.com/WesleyOne
 * @create 2021/11/28
 */
public class ObjectTest {

    static class Obj {
        private int age;

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }
    public static void main(String[] args) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, NoSuchFieldException {
        Obj obj = new Obj();
        Class<?> clz = obj.getClass();
        Field field = clz.getField("age");
        field.set(obj, 1);
    }
}
