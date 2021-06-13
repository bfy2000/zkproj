
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;

public class RmiClient {


    public static void rmiCall(String clause,String IPAddr, int opType, Long timeStamp, String tableName) throws Exception{

        // 如果RMI Registry就在本地机器上，URL就是:rmi://localhost:1099/hello
        // 否则，URL就是：rmi://RMIService_IP:1099/hello
        Registry registry = LocateRegistry.getRegistry("localhost");
        // 从Registry中检索远程对象的存根/代理
        RmiInterfaces remoteQuery = (RmiInterfaces)registry.lookup("RmiMethods");
        // 调用远程对象的方法
        String x = remoteQuery.callSQL(clause, IPAddr, opType, timeStamp, tableName);
        System.out.println(x);
    }

    public static ArrayList<SQLTimePair> dataSyncRequest(String IPAddr, Long timeStamp, String tableName) throws Exception{

        // 如果RMI Registry就在本地机器上，URL就是:rmi://localhost:1099/hello
        // 否则，URL就是：rmi://RMIService_IP:1099/hello
        Registry registry = LocateRegistry.getRegistry("rmi://localhost:1099/RmiMethods");
        // 从Registry中检索远程对象的存根/代理
        RmiInterfaces remoteQuery = (RmiInterfaces)registry.lookup("handleDataSync");
        // 调用远程对象的方法
        return remoteQuery.handleDataSync(IPAddr, timeStamp,tableName);

    }

}
