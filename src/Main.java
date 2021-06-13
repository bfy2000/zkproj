

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
                if(!mainCopyIP.equals(MyNodeInfo.getIPAddr())){
                    ArrayList<SQLTimePair> ops = RmiClient.dataSyncRequest(table,MyNodeInfo.getNewestTimeStamp(),table);
                    for(int i = 0; i<ops.size();i++){
                        int opType = 1;

                        if (ops.get(i).clause.split(" ")[1].equals("table".toUpperCase())){
                            if (ops.get(i).clause.split(" ")[0].equals("create".toUpperCase())){
                                opType = 2;
                            }
                            else if (ops.get(i).clause.split(" ")[0].equals("drop".toUpperCase())){
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
////            queryResult= Connector.connectDB("insert into testTable values(1);");
//            RmiClient.rmiCall(" CREATE TABLE Test1(id int);","localhost", 1, 0L, "Test1");
////            System.out.println(queryResult);
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }
}