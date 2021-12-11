package io.wesleyone;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.spi.ObjectFactory;
import java.util.Hashtable;

/**
 * @author https://github.com/WesleyOne
 * @create 2021/12/11
 */
public class UnsafeObject implements ObjectFactory {
    static {
        destroy();
    }

    private static void destroy() {
        System.out.println("====destroy=====");
    }

    @Override
    public Object getObjectInstance(Object obj, Name name, Context nameCtx, Hashtable<?, ?> environment) throws Exception {
        System.out.println("-----getObjectInstance");
        return null;
    }
}
