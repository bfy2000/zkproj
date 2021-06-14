
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;

public class RmiClient {


    public static void rmiCall(String clause,String IPAddr, int opType, Long timeStamp, String tableName) {

        // 如果RMI Registry就在本地机器上，URL就是:rmi://localhost:1099/hello
        // 否则，URL就是：rmi://RMIService_IP:1099/hello
        Registry registry = null;
        try {
            registry = LocateRegistry.getRegistry(IPAddr);
        } catch (RemoteException e) {
            e.printStackTrace();
        }
        // 从Registry中检索远程对象的存根/代理
        RmiInterfaces remoteQuery = null;
        try {
            remoteQuery = (RmiInterfaces)registry.lookup("RmiMethods");
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (NotBoundException e) {
            e.printStackTrace();
        }
        // 调用远程对象的方法
        String x = null;
        try {
            x = remoteQuery.callSQL(clause, opType, timeStamp, tableName);
        } catch (RemoteException e) {
            e.printStackTrace();
        }
        System.out.println(x);
    }

    public static ArrayList<SQLTimePair> dataSyncRequest(String IPAddr, Long timeStamp, String tableName)  {

        // 如果RMI Registry就在本地机器上，URL就是:rmi://localhost:1099/hello
        // 否则，URL就是：rmi://RMIService_IP:1099/hello
        System.out.println("开始同步");
        Registry registry = null;
        try {
            registry = LocateRegistry.getRegistry(IPAddr);
        } catch (RemoteException e) {
            e.printStackTrace();
        }
        // 从Registry中检索远程对象的存根/代理
        RmiInterfaces remoteQuery = null;
        try {
            remoteQuery = (RmiInterfaces)registry.lookup("RmiMethods");
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (NotBoundException e) {
            e.printStackTrace();
        }
        // 调用远程对象的方法
        try {
            return remoteQuery.handleDataSync(IPAddr, timeStamp,tableName);
        } catch (RemoteException e) {
            e.printStackTrace();
        }
        return new ArrayList<SQLTimePair>();
    }

}
