
import java.io.*;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class RmiMethods extends UnicastRemoteObject implements RmiInterfaces{

    private static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    protected RmiMethods() throws RemoteException {

    }
    //语句、目标地址、操作类型0读1写2建表3删除表
    @Override
    public String callSQL(String clause, String IPAddr, int opType, long timeStamp, String tableName) throws RemoteException {

        String queryResult = "initial";

        System.out.println(1);
        //读
        if (opType == 0){
            lock.readLock().lock();

            try {
                System.out.println(5);
                queryResult = Connector.connectDB(clause);
                if(queryResult.charAt(0) == '0'){
                    throw new Exception("queryError");
                }
                System.out.println(6);
            }
            catch (Exception e) {
                e.printStackTrace();
                System.out.println("数据库查询失败");
            }
            lock.readLock().unlock();
        }
        //写
        else {
            lock.writeLock().lock();

            //先自己执行
            try {
                System.out.println(5);
                queryResult = Connector.connectDB(clause);
                if(queryResult.charAt(0) == '0'){
                    throw new Exception("queryError");
                }
                System.out.println(6);
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("数据库查询失败");
            }

            //如果是一般写或删表
            if(opType == 1 || opType == 3){

                //执行完成后，如果成功要做相应操作
                if (queryResult.charAt(0) == '1') {

                    //删表还要额外删一下zookeeper和本地记录的
                    if(opType == 3) {
                        //更新本地记录
                        try {
                            Connector.getTables();
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.out.println("删表后更新本地记录失败");
                        }
                        //更新zookeeper
                        try {
                            //这个只有主节点做，其他表监听到自动同步
                            if(ServerSession.isMainCopy(tableName)){
                                //删maincopy
                                ServerSession.deleteMainCopy(tableName);
                                //删自己节点的
                                ServerSession.deleteTable();
                            }

                        } catch (Exception e) {
                            e.printStackTrace();
                            System.out.println("判断zookeeper主表失败，导致删表失败");
                        }
                    }
                }
                //第一个字符是0，执行失败
                else{
                    lock.writeLock().unlock();
                    return queryResult.substring(1);
                }

                //主节点让从节点执行
                try {
                    if(ServerSession.isMainCopy(tableName)){
                        //获得所有从节点，不会获取到自己
                        ArrayList<String> slaveNodes = ServerSession.getSlaveNodes(tableName);
                        //call从节点执行
                        for (String slaveNode:slaveNodes){
                            System.out.println(slaveNode+"\n");
                            RmiClient.rmiCall(clause, slaveNode, opType, timeStamp, tableName);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("执行成功，但向从节点发送语句失败");
                    //return "执行成功，但向从节点发送语句失败";
                }

            }

            //建表，本地加
            if(opType == 2){

                //这里要加一个判断，如果操作没成功就不做了
                if(queryResult.charAt(0) == '1'){

                    //在zookeeper注册新节点并更新本地记录
                    try {
                        Connector.getTables();
                        String tables = MyNodeInfo.getMyTables();
                        ServerSession.RegisterTables(tables);
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.out.println("抢占主副本失败，或注册新表失败，或本地数据更新失败");
                    }
                }
                //第一个字符是0，执行失败
                else{
                    lock.writeLock().unlock();
                    return queryResult.substring(1);
                }
            }

            //写，先写日志再解锁
            //处理字符串
            String filePath = "D:/" + tableName + "Log.txt";
            String content = clause.replace('\n',' ') + '\n';
            String timeStr = timeStamp + "";
            timeStr += '\n';

            System.out.println(2);
            //写文件
            try {


                File file = new File(filePath);
                if (!file.exists()) {
                    System.out.println("文件不存在，创建文件:" + filePath);
                    try {
                        file.createNewFile();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    System.out.println("文件已存在，文件为:" + filePath);
                }

                FileWriter fw = new FileWriter(filePath, true);
                fw.write(content.toLowerCase());
                fw.write(timeStr);
//                FileOutputStream fileOutputStream = new FileOutputStream(file);
//                fileOutputStream.write(content);
//                fileOutputStream.write(timeStr);

                //这里更新timestamp
                MyNodeInfo.setNewestTimeStamp(timeStamp);

                fw.close();
            }
            catch(Exception e){
                System.out.println("写日志错误");
                lock.writeLock().unlock();
                return queryResult.substring(1);
            }

            //更新时间戳
            if (timeStamp > MyNodeInfo.getNewestTimeStamp()){
                MyNodeInfo.setNewestTimeStamp(timeStamp);
            }

            System.out.println(7);
            //解锁
            lock.writeLock().unlock();
            System.out.println(8);

        }
        System.out.println(9);
        return queryResult.substring(1);
    }



    @Override
    public ArrayList<SQLTimePair> handleDataSync(String IPAddr, long timeStamp, String tableName) throws RemoteException {

        //锁住
        lock.writeLock().lock();
        System.out.println("开始同步");
        ArrayList<SQLTimePair> tableRec=new ArrayList<>();
        //读文件
        try{
            //打开文件
            String filePath = "D:/" + tableName + "Log.txt";
            FileInputStream fileInputStream = new FileInputStream(filePath);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));

            String content = null;
            long time = 0L;

            //读到需要的时间戳为止
            while (true) {
                content = bufferedReader.readLine();

                //找不到大于时间戳的时间，或者日志信息为空
                if(content == null){
                    tableRec.add(new SQLTimePair(0L,null));
                    break;
                }
                String content2 = bufferedReader.readLine();
                time = (long) Long.parseLong(content2);

                //获取到了第一个大于时间戳的时间
                if(time > timeStamp){
                    break;
                }
            }

            //一张表的日志
            if(time > timeStamp)
                tableRec.add(new SQLTimePair(time,content));
            while (true){

                content = bufferedReader.readLine();

                //完成一个表的导入
                if(content == null){
                    break;
                }
                time = Long.parseLong(bufferedReader.readLine());
                tableRec.add(new SQLTimePair(time,content));
            }
            lock.writeLock().unlock();

        }
        catch(Exception e){
            tableRec = new ArrayList<>();
//            tableRec.add(new SQLTimePair(0L,null));
            System.out.println("读日志错误");
            lock.writeLock().unlock();
        }


        return tableRec;

    }

}
