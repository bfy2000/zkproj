
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class RmiServer {
    public static void registerRmiServer() throws Exception{
        // 注册远程对象,向客户端提供远程对象服务。
        // 远程对象是在远程服务上创建的，你无法确切地知道远程服务器上的对象的名称，
        // 但是,将远程对象注册到RMI Registry之后,
        // 客户端就可以通过RMI Registry请求到该远程服务对象的stub，
        // 利用stub代理就可以访问远程服务对象了。
        RmiMethods remoteQuery = new RmiMethods();
        LocateRegistry.createRegistry(1099);
        Registry registry = LocateRegistry.getRegistry();
        registry.bind("RmiMethods", remoteQuery);
        // 如果不想再让该对象被继续调用，使用下面一行
        // UnicastRemoteObject.unexportObject(remoteMath, false);
    }
}
