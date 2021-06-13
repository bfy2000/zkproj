
import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.net.Socket;

public class Connector {

	public static String connectDB(String order) throws Exception {

		Socket socket = new Socket("localhost",3691);
		// ����DatagramSocket����

		System.out.println(3);
		if(!order.equals(""))
			order = order.substring(0,order.length()-1);

		//向服务器发送信息
		OutputStream os = socket.getOutputStream();
		PrintWriter pw = new PrintWriter(os);
		pw.write(order);
		pw.flush();
		socket.shutdownOutput();

		InputStream is = socket.getInputStream();
		BufferedReader br = new BufferedReader(new InputStreamReader(is));

		String queryResult = "";


		while(true){
			String tmp = br.readLine();
			if(tmp.equals(""))
				break;
			queryResult += tmp;
			queryResult += "\n";
		}

		System.out.println(4);

		br.close();
		is.close();
		os.close();
		pw.close();
		// ����ָ��
		socket.close();
		return queryResult;
	}

	public static void getTables() throws Exception {
		//InetAddress hostIP = InetAddress.getByName("localhost");
		// ��ȡĿ�걾��IP
		int hostPort = 3691;
		// ��ȡSQL�Ķ˿ںţ�

		Socket socket = new Socket("localhost",3691);
		// ����DatagramSocket����

		//向服务器发送信息
		OutputStream os = socket.getOutputStream();
		PrintWriter pw = new PrintWriter(os);
		pw.write("getTable");
		pw.flush();
		socket.shutdownOutput();

		InputStream is = socket.getInputStream();
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		String info = null;
		String tnames;
		tnames = br.readLine();

		if(tnames.charAt(0) == '0'){
			throw new Exception("getTable Failied");
		}
		else if (!tnames.equals("1")){
			tnames = tnames.substring(0,tnames.length()-1);
		}
		MyNodeInfo.setMyTables(tnames.substring(1));


		br.close();
		is.close();
		os.close();
		pw.close();
		// ����ָ��
		socket.close();
	}

}