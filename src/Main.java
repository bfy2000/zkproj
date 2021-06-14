

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) {

        Scanner in=new Scanner(System.in);
        String serverIP = in.nextLine();
        MyNodeInfo.setServerIP(serverIP);

        //更新本地表
        try {
            Connector.getTables();
        } catch (Exception e) {
            e.printStackTrace();
        }

        //注册rmi服务
        try {
            RmiMethods methods = new RmiMethods();
            LocateRegistry.createRegistry(1099);
            Registry registry=LocateRegistry.getRegistry();
            registry.bind("RmiMethods",methods);
            System.out.println("RMI APIs ready.");
        }
        catch (Exception e) {
            e.printStackTrace();
        }



//        //连接服务器
//        try {
//            ServerSession.connectServer(MyNodeInfo.getServerIP());
//        } catch (Exception e) {
//            e.printStackTrace();
//            System.out.print("connect server failed\n");
//        }


//      同步节点
        try {
            //每个表都进行同步
            for(String table:(MyNodeInfo.getMyTables()).split(",")){
                if(table.equals(""))
                    continue;
                String mainCopyIP = ServerSession.getMainCopy(table);

                String filePath = "D:/" + table + "Log.txt";
                FileInputStream fileInputStream = new FileInputStream(filePath);
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));

                String content = null;
                long time = 0L;

                //读到需要的时间戳为止
                while (true) {
                    content = bufferedReader.readLine();

                    //找不到大于时间戳的时间，或者日志信息为空
                    if(content == null){
                        break;
                    }
                    time = Long.parseLong(bufferedReader.readLine());
                }
                MyNodeInfo.setNewestTimeStamp(time);

                if(!mainCopyIP.equals(MyNodeInfo.getIPAddr())){
                    ArrayList<SQLTimePair> ops = RmiClient.dataSyncRequest(mainCopyIP,MyNodeInfo.getNewestTimeStamp(),table);
                    for(int i = 0; i<ops.size();i++){
                        int opType = 1;
                        if(ops.get(i).clause == null) {
                            continue;
                        }
                        if (ops.get(i).clause.split(" ")[2].equals("table".toLowerCase())){
                            if (ops.get(i).clause.split(" ")[1].equals("create".toLowerCase())){
                                opType = 2;
                            }
                            else if (ops.get(i).clause.split(" ")[1].equals("drop".toLowerCase())){
                                opType = 3;
                            }
                        }

                        RmiClient.rmiCall(ops.get(i).clause,MyNodeInfo.getIPAddr(),opType,ops.get(i).timeStamp,table);
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.out.print("sync failed\n");
            System.out.println("可能第一个节点上线时没有主副本导致，不影响");
        }

        //注册节点
        try {
            ServerSession.registerNode();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.print("register node failed\n");
        }

        //注册数据库表
        try {
            ServerSession.RegisterTables(MyNodeInfo.getMyTables());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.print("抢主节点失败\n");
        }

//        try {
//            String queryResult ;
////            queryResult= Connector.connectDB("CREATE TABLE testTable(id int);");
////            queryResult= Connector.connectDB();
////            queryResult= Connector.connectDB("insert into testTable val192.168.137.219ues(1);");
//            RmiClient.rmiCall(" select * from usb;","localhost", 0, 0L, "usb");
////            System.out.println(queryResult);
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }
}
