package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.graphics.Path;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.Context.TELEPHONY_SERVICE;

public class SimpleDynamoProvider extends ContentProvider {

	SharedPreferences sharedPref;
	Context context = getContext();
	SharedPreferences.Editor editor;
	Uri mUri;

	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final String [] REMOTE_PORTS = {"11108", "11112", "11116", "11120","11124"};
	static final String[] AVD = {"5554", "5556", "5558", "5560", "5562"};
	static final int SERVER_PORT = 10000;

	String selfPort;
	ReentrantReadWriteLock lock;

	TreeSet<Node> NODE_SET = new TreeSet<Node>();

	static final String ROOT_PORT = "5554";

	Node class_node = null;


	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		Log.d("DELETE", selection);
		if(selection.equals("*")){
			return 0;
		}else if(selection.equals("@")){
			editor.clear();
			editor.commit();
		}else{
			String hashed_key = null;
			List<Node> target_nodes = null;
			try {
				hashed_key = genHash(selection);
				target_nodes = get_target_node_list(hashed_key);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}

			for(Node node: target_nodes){
				Log.d(Operation.DATA_DELETE, "initial target_node: "+ node.getPortStr() + " - "+hashed_key + " - "+class_node.getPortStr());
				Log.d("NODE TARGET", node.getPortStr()+ " - " + selection);
				if(node.getPortStr().equals(class_node.getPortStr())){
					if(sharedPref.contains(selection)){
						editor.remove(selection);
						editor.commit();
					}else{
						Log.d("DELETE", " Key Not found");
					}
				}else{
					delete_query_key(node.getPortStr(), hashed_key, selection);
				}
			}

		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

		String key = (String) values.get("key");
		String value = (String) values.get("value");

		List<Node> target_nodes = null;
		try {
			target_nodes = get_target_node_list(genHash(key));
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		Log.d("NODE", target_nodes.toString());

		for(Node node: target_nodes){
			Log.d("NODE TARGET", node.getPortStr()+ " - " + key);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, node.getPortStr(), Operation.DATA_ADD, key, value);
		}

		return uri;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		context = getContext().getApplicationContext();
		sharedPref = context.getSharedPreferences("prefkey", Context.MODE_PRIVATE);
		editor = sharedPref.edit();
		editor.clear();

        Log.d("TEST", sharedPref.getAll().toString());
		//	https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/locks/ReentrantReadWriteLock.html
		lock = new ReentrantReadWriteLock();

		TelephonyManager telManager = (TelephonyManager) context.getSystemService(TELEPHONY_SERVICE);
		String portStr = telManager.getLine1Number().substring(telManager.getLine1Number().length() - 4);
		String portNumber = String.valueOf((Integer.parseInt(portStr)) * 2);

		selfPort = portStr;

		System.out.println(portStr + " -- " + portNumber);


		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
			return false;
		}


		try {
			Node n1 = new Node(genHash("5554"), genHash("5558"), genHash("5556"), "5554", "5558");
			Node n2 = new Node(genHash("5556"), genHash("5554"), genHash("5562"), "5556", "5554");
			Node n3 = new Node(genHash("5558"), genHash("5560"), genHash("5554"), "5558", "5560");
			Node n4 = new Node(genHash("5560"), genHash("5562"), genHash("5558"), "5560", "5562");
			Node n5 = new Node(genHash("5562"), genHash("5556"), genHash("5560"), "5562", "5556");

			NODE_SET.add(n1);
			NODE_SET.add(n2);
			NODE_SET.add(n3);
			NODE_SET.add(n4);
			NODE_SET.add(n5);

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		Iterator iter = NODE_SET.iterator();
		while (iter.hasNext()){
			Node n = (Node) iter.next();
			if(n.getPortStr().equals(portStr)){
				Log.d("CLASS NODE", "EQUAL");
				class_node = n;
			}
			System.out.println(n.getInfo());
		}

		Log.d("CLASS NODE", class_node.getInfo());

//		try {
//			String node_id  = genHash(portStr);
//			class_node = new Node(node_id, node_id, node_id, portStr, portStr);
//			if(!portStr.equals(ROOT_PORT)){
////                Log.d(TAG, "Not Equal ports");
//				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, portStr, Operation.NODE_ADD);
//			}else{
//				NODE_SET.add(class_node);
//			}
//		} catch (NoSuchAlgorithmException e) {
//			e.printStackTrace();
//		}
		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		Log.d("QUERY", selection);
		if(selection.equals("*")){
//            Log.d("QUERY", selection);
			// return Global key values

			// Get all keys and values from each Node
//                Log.d("QUERY", "get from all nodes");
			MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
			List<String> all_data = new ArrayList<String>();

			for(Node node: NODE_SET){
				String data = null;
				try {
					data = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, node.getPortStr(), Operation.DATA_QUERY).get();
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
				Log.d(Operation.DATA_QUERY, data);
				String[] pairs = data.split(",");
				for (String pair:pairs) {
//					Log.d("PAIR", pair);
					String[] keyValue = pair.split("=");
					if(!keyValue[0].trim().equals("") && !keyValue[1].trim().equals("")){
						MatrixCursor.RowBuilder rowBuilder = cursor.newRow();
						rowBuilder.add(keyValue[0].trim());
						rowBuilder.add(keyValue[1].trim());
					}
				}
				all_data.add(data);
			}
			return cursor;

		}else if(selection.equals("@")){
			MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
			Map<String, ?> stored_data = sharedPref.getAll();
			Log.d(Operation.DATA_QUERY, stored_data.toString());
			for (Map.Entry<String, ?> data : stored_data.entrySet()){
				MatrixCursor.RowBuilder rowBuilder = cursor.newRow();
//                Log.d("QUERY", data.getKey()+" -- "+data.getValue());
				rowBuilder.add(data.getKey());
				rowBuilder.add(data.getValue());
			}
			return cursor;
		}else{
			String target_node_port = null;
			String hashed_key = null;
			try {
				hashed_key = genHash(selection);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			target_node_port = get_target_node_list(hashed_key).get(0).getPortStr();
                Log.d(Operation.DATA_FIND_KEY, "initial target_node: "+ target_node_port + " - "+hashed_key +" - "+class_node.getPortStr());
			String value = null;
			// check if target node is current node and store locally else pass to successor
			if(target_node_port.equals(class_node.getPortStr())){
				value = sharedPref.getString(selection, null);
			}else{
				//pass to right node
				value = data_query_key(target_node_port, hashed_key, selection);
//                    Log.d(Operation.DATA_FIND_KEY, "from successor: "+value);
			}
			MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
			MatrixCursor.RowBuilder rowBuilder = cursor.newRow();
			rowBuilder.add(selection);
			rowBuilder.add(value);
			return cursor;
		}
//		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

	private List<Node> get_target_node_list(String hashed_key){
		List<Node> target_nodes  = new ArrayList<Node>();
		Node target = null, target_next, target_next_to_next;

		//Find out the right target node
		Node moving_node = NODE_SET.first();
		while (target == null) {
			if (hashed_key.compareTo(moving_node.getId()) < 0) {
				if (hashed_key.compareTo(moving_node.getPredecessor()) < 0 && moving_node.getId().compareTo(moving_node.getPredecessor()) < 0) {
					target = moving_node;
				} else if (hashed_key.compareTo(moving_node.getPredecessor()) > 0) {
					target = moving_node;
				} else {
					moving_node = NODE_SET.higher(moving_node);
				}
			} else if (hashed_key.compareTo(moving_node.getId()) > 0) {
				if (hashed_key.compareTo(moving_node.getPredecessor()) > 0 && moving_node.getId().compareTo(moving_node.getPredecessor()) < 0) {
					target = moving_node;
				} else {
					moving_node = NODE_SET.higher(moving_node);
				}
			}
		}

		target_next = (NODE_SET.higher(target) != null) ? NODE_SET.higher(target) : NODE_SET.first();
		target_next_to_next = (NODE_SET.higher(target_next) != null) ? NODE_SET.higher(target_next) : NODE_SET.first();

		target_nodes.add(target);
		target_nodes.add(target_next);
		target_nodes.add(target_next_to_next);

		return target_nodes;
	}


	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {

			ServerSocket serverSocket = sockets[0];
//          https://docs.oracle.com/javase/tutorial/networking/sockets/readingWriting.html
			try {
				do {
					Socket s = serverSocket.accept();
					Log.d("SERVER", "Server Socket Accept");
					BufferedReader data_in = new BufferedReader(new InputStreamReader(s.getInputStream()));

					String message;
					if ((message = data_in.readLine()) != null) {
						Log.d("SERVER", message);
						if(message.contains(Operation.NODE_ADD)) {
							String[] messageDetails = message.split(":");
							String portStr = messageDetails[1];
//                            Add Node to ring
							String node_id  = genHash(portStr);
							Node node = new Node(node_id, null, null, portStr, null);
							NODE_SET.add(node);
							Iterator iter = NODE_SET.iterator();

							Node CURRENT_NODE = node;
							Node PREVIOUS_NODE;
							Node NEXT_NODE;


							if(CURRENT_NODE.getId().equals(NODE_SET.first().getId())){
								PREVIOUS_NODE = NODE_SET.last();
								NEXT_NODE = NODE_SET.higher(CURRENT_NODE);
							}else if(CURRENT_NODE.getId().equals(NODE_SET.last().getId())){
								PREVIOUS_NODE = NODE_SET.lower(node);
								NEXT_NODE = NODE_SET.first();
							}else{
								PREVIOUS_NODE = NODE_SET.lower(node);
								NEXT_NODE = NODE_SET.higher(CURRENT_NODE);
							}

							System.out.println(" NODE DETAILS: "+PREVIOUS_NODE.getPortStr()+" - "+CURRENT_NODE.getPortStr()+" - "+NEXT_NODE.getPortStr());

							PREVIOUS_NODE.setSuccessor(CURRENT_NODE.getId());
							PREVIOUS_NODE.setSuccessorPort(CURRENT_NODE.getPortStr());
							CURRENT_NODE.setPredecessor(PREVIOUS_NODE.getId());
							CURRENT_NODE.setSuccessor(NEXT_NODE.getId());
							CURRENT_NODE.setSuccessorPort(NEXT_NODE.getPortStr());
							NEXT_NODE.setPredecessor(CURRENT_NODE.getId());

							String stream_msg = "NEW";
							while (iter.hasNext()){
								Node n = (Node) iter.next();
								System.out.println(n.getInfo());
								if(stream_msg.contains("NEW")){
									stream_msg = n.getInfo();
								}else {
									stream_msg = stream_msg+"%"+n.getInfo();
								}
							}

							node_join_update(stream_msg);

							PrintWriter data_out = new PrintWriter(s.getOutputStream(), true);
							data_out.println(Operation.NODE_ADD);
							data_out.close();

						}else if(message.contains(Operation.NODE_UPDATE)){
							String[] messageDetails = message.split(":");
//                            String portStr = messageDetails[1];
							String stream_msg = messageDetails[2];

//                          update node details
							NODE_SET.clear();

							String[] nodes = stream_msg.split("%");

							for(String node: nodes){
								String[] node_details = node.split("-");
//								id+" - "+portStr+" - "+predecessor+" - "+successor+" - "+successorPort;
								String id = node_details[0];
								String portStr = node_details[1];
								String predecessor = node_details[2];
								String successor = node_details[3];
								String successorPort = node_details[4];
								Node n = new Node(id, successor, predecessor, portStr, successorPort);
								NODE_SET.add(n);
								if(class_node.getId().equals(id)){
									class_node = n;
								}
							}

							Log.d(Operation.NODE_UPDATE, "SERVER NODE UPDATE");
							Iterator iter = NODE_SET.iterator();
							while (iter.hasNext()){
								Node n = (Node) iter.next();
								System.out.println(n.getInfo());
							}

							Log.d("NODE_UPDATE", class_node.getInfo());
							PrintWriter data_out = new PrintWriter(s.getOutputStream(), true);
							data_out.println(Operation.NODE_UPDATE);
							data_out.close();

						}else if(message.contains(Operation.DATA_ADD)){
							String[] messageDetails = message.split(":");
							String key = messageDetails[2];
							String value = messageDetails[3];

							add_data(key, value);

							PrintWriter data_out = new PrintWriter(s.getOutputStream(), true);
							data_out.println(Operation.DATA_ADD);
							data_out.close();

						}else if(message.contains(Operation.DATA_QUERY)){
							Map<String, String> stored_local_data = (Map<String, String>) sharedPref.getAll();

							String stored_data_string = "";
							if(!stored_local_data.isEmpty()){
								stored_data_string = stored_local_data.toString();
								stored_data_string = stored_data_string.substring(1, stored_data_string.length()-1);
							}
							PrintWriter data_out = new PrintWriter(s.getOutputStream(), true);
							Log.d(Operation.DATA_QUERY, stored_data_string);
							data_out.println(Operation.DATA_QUERY+":"+stored_data_string);
							data_out.close();

						}else if(message.contains(Operation.DATA_FIND_KEY)){
							String[] messageDetails = message.split(":");
							String portStr = messageDetails[1];
							String hashed_key = messageDetails[2];
							String key = messageDetails[3];

							PrintWriter data_out = new PrintWriter(s.getOutputStream(), true);
							Log.d("SERVER","Target_node data query key: "+ portStr);
							// check if target node is current node and store locally else pass to successor
							String value = sharedPref.getString(key, null);
							Log.d(Operation.DATA_FIND_KEY, "value: "+ value);
							data_out.println(Operation.DATA_FIND_KEY+":"+value);
							data_out.close();

						}else if(message.contains(Operation.DATA_DELETE)){


						}else if(message.contains(Operation.DATA_REMOVE_KEY)) {
							String[] messageDetails = message.split(":");
							String portStr = messageDetails[1];
							String hashed_key = messageDetails[2];
							String key = messageDetails[3];

							PrintWriter data_out = new PrintWriter(s.getOutputStream(), true);
							if(sharedPref.contains(key)){
								editor.remove(key);
								editor.commit();
							}else{
								Log.d("SERVER", Operation.DATA_REMOVE_KEY + " Key Not found");
							}
							data_out.println(Operation.DATA_REMOVE_KEY);
							data_out.close();
						}

						s.close();
					}
				} while (true);

			} catch (IOException e) {
				e.printStackTrace();
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}

			return null;
		}

	} //End of ServerTask


	private class ClientTask extends AsyncTask<String, Void, String> {

		@Override
		protected String doInBackground(String... msgs) {

			String portStr = msgs[0];
			String operation = msgs[1];

			String result= "";

//            Log.d("CLIENT", "Operation: "+operation);

			Socket socket = null;
			String msgToSend = "";
			try {
				//Check if root_node or forward to successor
				if(Operation.NODE_ADD.equals(operation)){
					String node_id  = genHash(portStr);
					Node node = new Node(node_id, node_id, node_id, portStr, portStr);
					System.out.println(node.getInfo());
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(ROOT_PORT)*2);
					msgToSend = Operation.NODE_ADD+":"+portStr;

				}else if(operation.equals(Operation.NODE_UPDATE)){
					String stream_msg = msgs[2];
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(portStr)*2);
					msgToSend = Operation.NODE_UPDATE + ":" + portStr + ":" + stream_msg;

				}else if(operation.equals(Operation.DATA_ADD)){
					String key = msgs[2];
					String value = msgs[3];
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(portStr)*2);
					msgToSend = Operation.DATA_ADD + ":" + portStr + ":" + key + ":" + value;

				}else if(operation.equals(Operation.DATA_QUERY)){
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(portStr)*2);
					msgToSend = Operation.DATA_QUERY + ":" + portStr;

				}else if(operation.equals(Operation.DATA_FIND_KEY)){
					String hashed_key = msgs[2];
					String key = msgs[3];
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(portStr)*2);
					msgToSend = Operation.DATA_FIND_KEY + ":" + portStr + ":" + hashed_key + ":" + key;

				}else if(operation.equals(Operation.DATA_DELETE)){

				}else if(operation.equals(Operation.DATA_REMOVE_KEY)){
					String hashed_key = msgs[2];
					String key = msgs[3];
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(portStr)*2);
					msgToSend = Operation.DATA_REMOVE_KEY + ":" + portStr + ":" + hashed_key + ":" + key;
				}


				PrintWriter data_out = new PrintWriter(socket.getOutputStream(), true);

				data_out.println(msgToSend);
				Log.d("CLIENT", "Socket Message sent: "+msgToSend);

				BufferedReader data_in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				String message = data_in.readLine();
				Log.d("CLIENT", "Client Socket receive " + message);
				if (message != null && message.contains(Operation.DATA_QUERY)) {
					String [] data_from_server = message.split(":");
					String data = "";
					if(data_from_server.length > 1){
						data = data_from_server[1];
					}
					Log.d("CLIENT", "socket receive data query: "+data);
					if(data == "") System.out.println("BLANK");
					socket.close();
					return data;
				}else if (message != null && message.contains(Operation.DATA_FIND_KEY)) {
					String data = message.split(":")[1];
					socket.close();
					return data;
				}else{
					socket.close();
				}


			} catch (IOException e) {
				if(Operation.NODE_UPDATE.equals(operation)){
					Log.d("CONNECTION", "NODE UPDATE FAILED to port "+portStr);
				}else{
					e.printStackTrace();
				}
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			return result;
		}
	} //End of ClientTask


	private void add_data(String key, String value){
		try {
			lock.writeLock().lock();
			editor.putString(key, value);
			editor.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			lock.writeLock().unlock();
		}
	}

	private void node_join_update(String stream_msg){
		//Multicast
		for(int i = 0; i<AVD.length; i++) {
			String remotePort = AVD[i];
			if(!remotePort.equals(ROOT_PORT)) {
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, remotePort, Operation.NODE_UPDATE, stream_msg);
			}
		}
	} // node join update

	private String data_query_key(String target_node_port, String hashed_key, String key){
		try {
			String value = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, target_node_port, Operation.DATA_FIND_KEY, hashed_key, key).get();
			Log.d(Operation.DATA_FIND_KEY, "Fuction value: "+value);
			return value;
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return null;
	}

	private String delete_query_key(String target_node_port, String hashed_key, String key){
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, target_node_port, Operation.DATA_REMOVE_KEY, hashed_key, key);
		return null;
	}


}

class Node implements Comparable<Node> {
	private String id;
	private String predecessor;
	private String successor;
	private String portStr;
	private String successorPort;

	Node(String id, String successor, String predecessor, String portStr, String successorPort){
		this.id = id;
		this.successor = successor;
		this.predecessor = predecessor;
		this.portStr = portStr;
		this.successorPort  = successorPort;
	}

	public void setSuccessor(String successor) {
		this.successor = successor;
	}

	public void setSuccessorPort(String successorPort) {
		this.successorPort = successorPort;
	}

	public void setPredecessor(String predecessor) {
		this.predecessor = predecessor;
	}

	public String getId() {
		return id;
	}

	public String getPortStr() {
		return portStr;
	}

	public String getPredecessor() {
		return predecessor;
	}

	public String getSuccessor() {
		return successor;
	}

	public String getSuccessorPort() {
		return successorPort;
	}

	String getInfo(){
		return id+" - "+portStr+" - "+predecessor+" - "+successor+" - "+successorPort;
	}

	@Override
	public int compareTo(Node another) {
		return this.id.compareTo(another.id);
	}
}

class Operation{
	public static final String NODE_ADD = "NODE_ADD";
	public static final String NODE_UPDATE = "NODE_UPDATE";
	public static final String DATA_ADD = "DATA_ADD";
	public static final String DATA_QUERY = "DATA_QUERY";
	public static final String DATA_FIND_KEY = "DATA_FIND_KEY";
	public static final String QUERY_END = "QUERY_END";
	public static final String DATA_DELETE = "DATA_DELETE";
	public static final String DATA_REMOVE_KEY = "DATA_REMOVE_KEY";
}