
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;


public class ServerSession implements Watcher{

    private static ZooKeeper zk;
    private static Map<String,Stat> versionMap;
   // private static CountDownLatch countDownLatch = new CountDownLatch(1);

    static {
        try {
            //countDownLatch = new CountDownLatch(1);
            versionMap = new HashMap<String,Stat>();
            String serverIP = MyNodeInfo.getServerIP();
            zk = new ZooKeeper(serverIP, 5000, new Watcher(){
                @Override
                public void process(WatchedEvent event) {
                    if(Event.KeeperState.SyncConnected==event.getState()){
                        //首次连接成功
                        if (EventType.None == event.getType() && null == event.getPath()) {
                            System.out.println("Connect Zookeeper Server Success");
                            //countDownLatch.countDown();
                        }
                    }
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
        //连接成功
//        try {
//            countDownLatch.await();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
        System.out.println(zk.getState());
    }

    //实现watcher,用来监听Master节点
    @Override
    public void process(WatchedEvent event) {
        //主副本没了
        if(event.getType() == EventType.NodeDeleted){
            try {
                //抢主副本
                String mainCopyPath = event.getPath();
                //查看主副本存在性并注册一个一次性监听函数
                Stat mainCopyStat = zk.exists(mainCopyPath, false);
                //主副本不存在，抢注主副本
                if(mainCopyStat == null) {
                    zk.create(mainCopyPath, MyNodeInfo.getIPAddr().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                    System.out.println("抢到主副本");
                }
                else {
                    zk.exists(mainCopyPath, true);
                }

            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Get MainCopy Failed");
                //没抢到也要监听
                try {
                    zk.exists(event.getPath(),true);
                } catch (KeeperException | InterruptedException keeperException) {
                    keeperException.printStackTrace();
                }
            }
        }

        //删除节点
        if(event.getType() == EventType.NodeDataChanged){
            try {
                deleteTable();
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("在zookeeper删表出错");
            }
        }
    }

    //注册节点
    public static void registerNode() throws Exception{

        String IPAddr = MyNodeInfo.getIPAddr();
        String tables = MyNodeInfo.getMyTables();

        //注册自己
        String path ="/DBRoot/" + IPAddr;
        zk.create(path,tables.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        MyNodeInfo.setMyPath(path);
        System.out.println("注册节点成功");

    }

    //注册表
    public static void RegisterTables (String tables) throws Exception{

        String IPAddr = MyNodeInfo.getIPAddr();
        String prePath = MyNodeInfo.getMyPath();

        String[] tableList = tables.split(",");

        for (String table : tableList) {
            if(table.equals(""))
                continue;
            //抢主副本
            String mainCopyPath = "/MainCopy/" + table;
            //查看主副本存在性并注册一个一次性监听函数
            Stat mainCopyStat = zk.exists(mainCopyPath, false);
            //主副本不存在，抢注主副本
            if (mainCopyStat == null) {
                zk.create(mainCopyPath, IPAddr.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                System.out.println(prePath + "抢到主副本");
            } else {
                zk.exists(mainCopyPath, new ServerSession());
                System.out.println("没抢到主副本");
            }
        }

        //在自己的节点下注册表
        Connector.getTables();

        String path = "/DBRoot/" + IPAddr;
        if(!versionMap.containsKey(path)){
            versionMap.put(path,new Stat());
        }
        Stat newStat = zk.setData(path,MyNodeInfo.getMyTables().getBytes(),versionMap.get(path).getVersion());
        versionMap.replace(path,newStat);
//        Connector.getTables();
//
//        try{
//            zk.create(path, tables.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
//            System.out.println("表注册成功");
//        }
//        catch (Exception e){
//            e.printStackTrace();
//            System.out.println("表注册失败，尝试更新数据");
//            try{
//                zk.setData(path,tables.getBytes(),new Stat().getVersion());
//            }
//            catch (Exception ex){
//                System.out.println("更新数据失败");
//            }
//        }
    }

    public static Boolean isMainCopy(String tableName)throws Exception{
        String path = "/MainCopy/"+tableName;
        if(!versionMap.containsKey(path)){
            versionMap.put(path,new Stat());
        }
        String mainCopyIP = new String(zk.getData(path, false, versionMap.get(path)));
        return mainCopyIP.equals(MyNodeInfo.getIPAddr());
    }

    public static ArrayList<String> getSlaveNodes(String tableName){

        String IPAddr = MyNodeInfo.getIPAddr();
        ArrayList<String> slaveNodes = new ArrayList<>();

        try {
            List<String> nodeList = zk.getChildren("/DBRoot",null);
            //遍历node
            for(String node : nodeList){
                //是不是自己

                String path = "/DBRoot/"+node;
                if(!versionMap.containsKey(path)){
                    versionMap.put(path,new Stat());
                }

                String nodeIP = node;
                if (nodeIP.equals(IPAddr)){
                    continue;
                }

                //遍历表
                String tableList = new String(zk.getData(path,false,versionMap.get("/DBRoot/"+node)));
                for(String table : tableList.split(",")){
                    if(table.equals(tableName)){
                        slaveNodes.add(nodeIP);
                        break;
                    }
                }
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }

        return slaveNodes;
    }

    public static String getMainCopy (String tableName)throws Exception{
        String path = "/MainCopy/" + tableName;
//        if(!versionMap.containsKey(path)){
//            versionMap.put(path,new Stat());
//        }
        String res = new String(zk.getData(path,false, null/*versionMap.get(path)*/));
        return res;
    }

    public static void deleteTable(){
        Stat stat = new Stat();
        String tables = MyNodeInfo.getMyTables();
        try {
            zk.delete(MyNodeInfo.getMyPath(),stat.getVersion());
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            System.out.println("zookeeper删除自己的节点失败");
        }

    }

    public static void deleteMainCopy(String table){
        try {
            zk.delete("/MainCopy/"+table,new Stat().getVersion());
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
            System.out.println("zookeeper删除主节点失败");
        }
    }


}
