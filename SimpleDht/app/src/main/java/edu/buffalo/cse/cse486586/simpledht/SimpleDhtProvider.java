package edu.buffalo.cse.cse486586.simpledht;

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
import java.util.Comparator;
import java.util.Formatter;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

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



//    ContentValues cv = new ContentValues();
//            cv.put("key", strings[0]);
//            cv.put("value", strReceived);

//    getContentResolver().insert(mUri, cv);


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

        Set<String> valueSet = values.keySet();
        for (String stringObjectEntry : valueSet) {
//            Log.v("BIPUL", stringObjectEntry);
            String key = (String) values.get("key");
            String value = (String) values.get("value");
            editor.putString(key, value);
        }
        editor.commit();

        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        context = getContext().getApplicationContext();
        sharedPref = context.getSharedPreferences("prefkey", Context.MODE_PRIVATE);
        editor = sharedPref.edit();
        editor.clear();

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
            class_node = new Node(node_id, node_id, node_id, portStr);
            if(!portStr.equals(ROOT_PORT)){
                Log.d(TAG, "Not Equal ports");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, portStr, Operation.NODE_ADD);
            }else{
                NODE_SET.add(class_node);
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }



        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub

        MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
        MatrixCursor.RowBuilder rowBuilder = cursor.newRow();
        rowBuilder.add(selection);
        rowBuilder.add(sharedPref.getString(selection, null));

        return cursor;
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
                            Node node = new Node(node_id, null, null, portStr);
                            NODE_SET.add(node);
                            Iterator iter = NODE_SET.iterator();
                            while (iter.hasNext()){
                                Node n = (Node) iter.next();
                                System.out.println(n.getInfo());
                            }
                            Node CURRENT_NODE = node;
                            Node PREVIOUS_NODE = null;
                            Node NEXT_NODE = null;


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

                            PREVIOUS_NODE.setSuccessor(CURRENT_NODE.getId());
                            CURRENT_NODE.setPredecessor(PREVIOUS_NODE.getId());
                            CURRENT_NODE.setSuccessor(NEXT_NODE.getId());
                            NEXT_NODE.setPredecessor(CURRENT_NODE.getId());


                            node_join_update(PREVIOUS_NODE, "SUCC", CURRENT_NODE);
                            node_join_update(CURRENT_NODE, "PRED", PREVIOUS_NODE);
                            node_join_update(CURRENT_NODE, "SUCC", NEXT_NODE);
                            node_join_update(NEXT_NODE, "PRED", CURRENT_NODE);


//                            if(NODE_SET.size() == 1){
//                                Node n = NODE_SET.first();
//                                n.setSuccessor(node_id);
//                                n.setPredecessor(node_id);
//                                Node node = new Node(node_id, n.getId(), n.getId(), portStr);
//                                NODE_SET.add(node);
//                            }else{
//
//                            }
                        }else if(message.contains(Operation.NODE_UPDATE)){
                            String[] messageDetails = message.split(":");
//                            String portStr = messageDetails[1];
                            String source = messageDetails[2];
                            String type = messageDetails[3];
                            String destination = messageDetails[4];
//                          update node details

                            if(source.equals(class_node.getId())){
                                if(type.equals("SUCC")){
                                    class_node.setSuccessor(destination);
                                }
                                if(type.equals("PRED")){
                                    class_node.setPredecessor(destination);
                                }
                            }
                        }
                        Iterator iter = NODE_SET.iterator();
                        while (iter.hasNext()){
                            Node n = (Node) iter.next();
                            System.out.println(n.getInfo());
                        }

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


    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            String portStr = msgs[0];
            String operation = msgs[1];


            Socket socket = null;
            String msgToSend = "";
            try {
                //Check if root_node or forward to successor
                if(Operation.NODE_ADD.equals(operation)){
                    String node_id  = genHash(portStr);
                    Node node = new Node(node_id, node_id, node_id, portStr);
                    System.out.println(node.getInfo());
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(ROOT_PORT)*2);
                    msgToSend = Operation.NODE_ADD+":"+portStr;

                }else if(operation.equals(Operation.NODE_UPDATE)){
                    String source = msgs[2];
                    String type = msgs[3];
                    String destination = msgs[4];
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(portStr)*2);
                    msgToSend = Operation.NODE_UPDATE+":"+portStr+":"+source+":"+type+":"+destination;
                }

                PrintWriter data_out = new PrintWriter(socket.getOutputStream(), true);

                data_out.println(msgToSend);
                Log.d("CLIENT", "Socket Message sent");

            } catch (IOException e) {
                e.printStackTrace();
            }catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

            return null;
        }
    } //End of ClientTask


    private void node_join_update(Node source, String type, Node destination){
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, source.getPortStr(), Operation.NODE_UPDATE, source.getId(), type, destination.getId());
    }


}//End of Providerclass


class Node implements Comparable<Node> {
    private String id;
    private String predecessor;
    private String successor;
    private String portStr;

    Node(String id, String successor, String predecessor, String portStr){
        this.id = id;
        this.successor = successor;
        this.predecessor = predecessor;
        this.portStr = portStr;
    }

    public void setSuccessor(String successor) {
        this.successor = successor;
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

    String getInfo(){
        return id+" - "+portStr+" - "+predecessor+" - "+successor;
    }

    @Override
    public int compareTo(Node another) {
        return this.id.compareTo(another.id);
    }
}

class Operation{
    public static final String NODE_ADD = "NODE_ADD";
    public static final String NODE_UPDATE = "NODE_UPDATE";
    private static final String DATA_ADD = "DATA_ADD";
    private static final String QUERY_LOCAL = "QUERY_LOCAL";
    private static final String QUERY_GLOBAL = "QUERY_GLOBAL";
    private static final String DELETE_LOCAL = "DELETE_LOCAL";
    private static final String DELETE_GLOBAL = "DELETE_GLOBAL";
}
