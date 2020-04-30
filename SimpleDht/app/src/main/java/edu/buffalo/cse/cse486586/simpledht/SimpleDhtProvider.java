package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.SyncFailedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.Context.TELEPHONY_SERVICE;

public class SimpleDhtProvider extends ContentProvider {

    SharedPreferences sharedPref;
    Context context = getContext();
    SharedPreferences.Editor editor;
    Uri mUri;

    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final String[] REMOTE_PORTS = {"11108", "11112", "11116", "11120", "11124"};
    static final String[] AVD = {"5554", "5556", "5558", "5560", "5562"};
    static final int SERVER_PORT = 10000;

    static final String ROOT_PORT = "5554";

    Node class_node = null;

    TreeSet<Node> NODE_SET = new TreeSet<Node>();

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
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

//        Log.d("CLASS_NODE", class_node.getSuccessorPort()+" -- "+class_node.getPortStr());

        String key = (String) values.get("key");
        String value = (String) values.get("value");
        String hashed_key = null;
        try {
            hashed_key = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        if(class_node.getPredecessor().equals(class_node.getId()) && class_node.getSuccessor().equals(class_node.getId()) && NODE_SET.size() == 1){
            //Only one node in the ring
            Log.d("INSERT", "Single node: "+key+" - "+value);
            editor.putString(key, value);
            editor.commit();
        }else{
            // Check for right target node
            String target_node_port = get_target_node(hashed_key);
//            Log.d("INSERT", "Multi node: "+key+" - "+value+ "-> "+target_node_port);

            // check if target node is current node and store locally else pass to successor
            if(target_node_port.equals(class_node.getPortStr())){
                editor.putString(key, value);
                editor.commit();
            }else{
                //pass to successor
                data_join(target_node_port, hashed_key, key, value);
            }
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
//        Log.d("TEST", sharedPref.getAll().toString());
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority("edu.buffalo.cse.cse486586.simpledht.provider");
        uriBuilder.scheme("content");
        mUri = uriBuilder.build();

        TelephonyManager telManager = (TelephonyManager) context.getSystemService(TELEPHONY_SERVICE);
        String portStr = telManager.getLine1Number().substring(telManager.getLine1Number().length() - 4);
        String portNumber = String.valueOf((Integer.parseInt(portStr)) * 2);

        System.out.println(portStr + " -- " + portNumber);


        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }

        try {
            String node_id  = genHash(portStr);
            class_node = new Node(node_id, node_id, node_id, portStr, portStr);
            if(!portStr.equals(ROOT_PORT)){
                Log.d(TAG, "Not Equal ports");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, portStr, Operation.NODE_ADD);
            }else{
                NODE_SET.add(class_node);
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        Log.d("CLASS_NODE", class_node.getSuccessorPort()+" -- "+class_node.getPortStr());
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub
        if(selection.equals("*")){
            Log.d("QUERY", selection);
            // return Global key values
            if(class_node.getPredecessor().equals(class_node.getId()) && class_node.getSuccessor().equals(class_node.getId()) && NODE_SET.size() == 1){
                //Only one node in the ring
                MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
                Map<String, ?> stored_data = sharedPref.getAll();
                for (Map.Entry<String, ?> data : stored_data.entrySet()){
                    MatrixCursor.RowBuilder rowBuilder = cursor.newRow();
                    rowBuilder.add(data.getKey());
                    rowBuilder.add(data.getValue());
                }
                return cursor;
            }else{
                // Get all keys and values from each Node
                Log.d("QUERY", "get from all nodes");
                MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
                String data_from_node = fetch_global_data(class_node.getSuccessorPort(), class_node.getPortStr());
                Log.d("DATA FETCH", data_from_node);
                String[] pairs = data_from_node.split(",");
                for (int i = 0; i < pairs.length; i++) {
                    String pair = pairs[i];
                    String[] keyValue = pair.split("=");
                    MatrixCursor.RowBuilder rowBuilder = cursor.newRow();
                    rowBuilder.add(keyValue[0].trim());
                    rowBuilder.add(keyValue[1].trim());
                }
                return cursor;
            }
        }else if(selection.equals("@")){
            Log.d("QUERY", selection);
            // return local key values
            MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
            Map<String, ?> stored_data = sharedPref.getAll();
            for (Map.Entry<String, ?> data : stored_data.entrySet()){
                MatrixCursor.RowBuilder rowBuilder = cursor.newRow();
//                Log.d("QUERY", data.getKey()+" -- "+data.getValue());
                rowBuilder.add(data.getKey());
                rowBuilder.add(data.getValue());
            }
            return cursor;
        }else{
            MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
            MatrixCursor.RowBuilder rowBuilder = cursor.newRow();
            rowBuilder.add(selection);
            rowBuilder.add(sharedPref.getString(selection, null));
            return cursor;
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
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

    private String get_target_node(String hashed_key){
        String target_node_port = null;
        boolean base_node = false;
        if(hashed_key.compareTo(class_node.getId()) < 0){
            if(hashed_key.compareTo(class_node.getPredecessor()) < 0 && class_node.getId().compareTo(class_node.getPredecessor()) < 0){
                target_node_port = class_node.getPortStr();
            }else if(hashed_key.compareTo(class_node.getPredecessor()) > 0){
                target_node_port = class_node.getPortStr();
            }else{
                target_node_port = class_node.getSuccessorPort();
            }
        }else if(hashed_key.compareTo(class_node.getId()) > 0){
            if(hashed_key.compareTo(class_node.getPredecessor()) > 0 && class_node.getId().compareTo(class_node.getPredecessor()) < 0){
                target_node_port = class_node.getPortStr();
            }else{
                target_node_port = class_node.getSuccessorPort();
            }
        }
//        if(class_node.getPredecessor().compareTo(class_node.getId()) > 0){
//            base_node = true;
//        }
//        if(base_node){
//            if(hashed_key.compareTo(class_node.getId()) > 0 && hashed_key.compareTo(class_node.getPredecessor()) < 0 ){
//                target_node_port = class_node.getPortStr();
//            }else {
//                target_node_port = class_node.getSuccessorPort();
//            }
//        }else{
//            if(hashed_key.compareTo(class_node.getPredecessor()) > 0 && hashed_key.compareTo(class_node.getId()) < 0){
//                target_node_port = class_node.getPortStr();
//            }else{
//                target_node_port = class_node.getSuccessorPort();
//            }
//        }
        return target_node_port;
    }

    private String fetch_global_data(String successorPort, String origin_node) {
        // first get local data then move to successor
        Map<String, String> stored_local_data = (Map<String, String>) sharedPref.getAll();
        Log.d("fetch_global_data", String.valueOf(stored_local_data));
//        global_data.putAll(stored_local_data);
        String stored_data_string = stored_local_data.toString();
        stored_data_string = stored_data_string.substring(1, stored_data_string.length()-1);


//        String data_from_node = data_query(successorPort, origin_node);
        Log.d("STORED LOCAL DATA", stored_data_string);
        if (!successorPort.equals(origin_node)) {

            Map<String, String> data_from_node = new HashMap<String, String>();
            String data = data_query(successorPort, origin_node);
//            data = data.substring(1, data.length()-1);
            Log.d("DATA FETCHED", data);
            String[] pairs = data.split(",");
            System.out.println(pairs.length);
            for (String pair : pairs) {
                String[] keyValue = pair.split("=");
                data_from_node.put(keyValue[0], keyValue[1]);
            }
            Log.d("Mapped DATA", String.valueOf(data_from_node));
            stored_local_data.putAll(data_from_node);
            stored_data_string = stored_data_string.concat(data);
            Log.d("Concat DATA", stored_data_string);
        }
        return stored_data_string;
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
                            while (iter.hasNext()){
                                Node n = (Node) iter.next();
                                System.out.println(n.getInfo());
                            }
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


                            node_join_update(PREVIOUS_NODE, "SUCC", CURRENT_NODE);
                            node_join_update(CURRENT_NODE, "PRED", PREVIOUS_NODE);
                            node_join_update(CURRENT_NODE, "SUCC", NEXT_NODE);
                            node_join_update(NEXT_NODE, "PRED", CURRENT_NODE);

                            PrintWriter data_out = new PrintWriter(s.getOutputStream(), true);
                            data_out.println(Operation.NODE_ADD);
                            data_out.close();


                        }else if(message.contains(Operation.NODE_UPDATE)){
                            String[] messageDetails = message.split(":");
//                            String portStr = messageDetails[1];
                            String source = messageDetails[2];
                            String type = messageDetails[3];
                            String destination = messageDetails[4];
                            String sourceSuccPort = messageDetails[5];
//                          update node details

                            if(source.equals(class_node.getId())){
                                if(type.equals("SUCC")){
                                    class_node.setSuccessor(destination);
                                    class_node.setSuccessorPort(sourceSuccPort);
                                }
                                if(type.equals("PRED")){
                                    class_node.setPredecessor(destination);
                                }
                            }
                            Log.d("NODE_UPDATE", class_node.getInfo());
                            PrintWriter data_out = new PrintWriter(s.getOutputStream(), true);
                            data_out.println(Operation.NODE_UPDATE);
                            data_out.close();

                        }else if(message.contains(Operation.DATA_ADD)){
                            String[] messageDetails = message.split(":");
                            String portStr = messageDetails[1];
                            String hashed_key = messageDetails[2];
                            String key = messageDetails[3];
                            String value = messageDetails[4];
//                          Add data to right node

                            // Check for right target node
                            String target_node_port = get_target_node(hashed_key);
                            Log.d("SERVER","Target_node data add: "+ target_node_port);
                            // check if target node is current node and store locally else pass to successor
                            if(target_node_port.equals(class_node.getPortStr())){
                                editor.putString(key, value);
                                editor.commit();
                            }else{
                                //pass to successor
                                data_join(target_node_port, hashed_key, key, value);
                            }

                            PrintWriter data_out = new PrintWriter(s.getOutputStream(), true);
                            data_out.println(Operation.DATA_ADD);
                            data_out.close();

                        }else if(message.contains(Operation.DATA_QUERY)){
                            String[] messageDetails = message.split(":");
                            String successor_port = messageDetails[1];
                            String origin_node = messageDetails[2];
                            PrintWriter data_out = new PrintWriter(s.getOutputStream(), true);
                            Log.d("SERVER DATA_QUERY", successor_port+" - "+origin_node);
                            if(origin_node == class_node.getId()){
                                // return complete
                                data_out.println(Operation.QUERY_END);
                            }else{
                                //get successor data and return
                                String data = fetch_global_data(class_node.getSuccessorPort(), origin_node);
                                data_out.println(Operation.DATA_QUERY+":"+data);
                            }
                            data_out.close();
                        }
                        Iterator iter = NODE_SET.iterator();
                        while (iter.hasNext()){
                            Node n = (Node) iter.next();
                            System.out.println(n.getInfo());
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

            Log.d("CLIENT", "Operation: "+operation);

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
                    String source = msgs[2];
                    String type = msgs[3];
                    String destination = msgs[4];
                    String sourceSuccPort = msgs[5];
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(portStr)*2);
                    msgToSend = Operation.NODE_UPDATE + ":" + portStr + ":" + source + ":" + type + ":" + destination + ":" +sourceSuccPort;

                }else if(operation.equals(Operation.DATA_ADD)){
                    String hashed_key = msgs[2];
                    String key = msgs[3];
                    String value = msgs[4];
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(portStr)*2);
                    msgToSend = Operation.DATA_ADD + ":" + portStr + ":" + hashed_key + ":" + key + ":" + value;

                }else if(operation.equals(Operation.DATA_QUERY)){
                    String successorPort = msgs[0];
                    String origin_node = msgs[2];
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successorPort)*2);
                    Log.d(Operation.DATA_QUERY, "Data Query from port "+ successorPort + " - origin "+origin_node);
                    msgToSend = Operation.DATA_QUERY + ":" + successorPort + ":" + origin_node;

                    // Get localdata and keep merging from successor and return
                    Log.d(Operation.DATA_QUERY, "Data Query from port "+ successorPort);
                }

                PrintWriter data_out = new PrintWriter(socket.getOutputStream(), true);

                data_out.println(msgToSend);
                Log.d("CLIENT", "Socket Message sent: "+msgToSend);

                BufferedReader data_in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String message = data_in.readLine();
                Log.d("CLIENT", "Client Socket receive " + message);
                if (message != null && message.contains(Operation.DATA_QUERY)) {
                    String data = message.split(":")[1];
                    socket.close();
                    return data;
                }else{
                    socket.close();
                }


            } catch (IOException e) {
                e.printStackTrace();
            }catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            return result;
        }
    } //End of ClientTask


    private void node_join_update(Node source, String type, Node destination){
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, source.getPortStr(), Operation.NODE_UPDATE, source.getId(), type, destination.getId(), type.equals("SUCC")?destination.getPortStr():null);
    }

    private void data_join(String target_node_port, String hashed_key, String key, String value){
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, target_node_port, Operation.DATA_ADD, hashed_key, key, value);
    }
    private String data_query(String successorPort, String origin_node){
        Log.d(Operation.DATA_QUERY, "data_query - successorPort "+successorPort+" origin_node "+origin_node);
        try {
            String data = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, successorPort, Operation.DATA_QUERY, origin_node).get();
            Log.d(Operation.DATA_QUERY, data);
            return data;
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return null;
    }


}//End of Providerclass


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
    public static final String QUERY_END = "QUERY_END";
    public static final String DELETE_LOCAL = "DELETE_LOCAL";
    public static final String DELETE_GLOBAL = "DELETE_GLOBAL";
}
