
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;

public interface RmiInterfaces extends Remote{
    /**
     * 必须继承Remote接口。
     * 所有参数和返回类型必须序列化(因为要网络传输)。
     * 任意远程对象都必须实现此接口。
     * 只有远程接口中指定的方法可以被调用。
     */
    // 所有方法必须抛出RemoteException
    public String callSQL(String clause, String IPAddr, int opType, long timeStamp, String tableName) throws RemoteException;
    public ArrayList<SQLTimePair> handleDataSync(String IPAddr, long timeStamp, String tableName) throws RemoteException;

}
