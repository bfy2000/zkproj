import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.stream.IntStream;

public class MyNodeInfo {
    private static String IPaddr = "";
    private static String myPath = "";
    private static String ServerIP = "";
    private static Long newestTimeStamp = 0L;
    private static String myTables = "";

    static {
        try {
            int[] IPArray = new int[4];
            byte[] IPbyte = new byte[4];
            IPbyte = InetAddress.getLocalHost().getAddress();
            for(int i = 0; i < 4; i++){
                IPArray[i] = (int)IPbyte[i] & 0xff;
            }
            IPaddr = IPArray[0] + "." + IPArray[1] + "." + IPArray[2] + "." + IPArray[3];
        }
        catch (UnknownHostException e) {
            e.printStackTrace();
        }



    }


    public static void setMyPath(String path){
        myPath = path;
    }

    public static String getIPAddr(){
        return IPaddr;
    }

    public static String getMyPath(){
        return myPath;
    }

    public static String getServerIP() {
        return ServerIP;
    }

    public static void setServerIP(String serverIP) {
        ServerIP = serverIP;
    }

    public static Long getNewestTimeStamp() {
        return newestTimeStamp;
    }

    public static void setNewestTimeStamp(Long newestTimeStamp) {
        MyNodeInfo.newestTimeStamp = newestTimeStamp;
    }

    public static String getMyTables() {
        return myTables;
    }

    public static void setMyTables(String myTables) {
        MyNodeInfo.myTables = myTables;
    }
}
