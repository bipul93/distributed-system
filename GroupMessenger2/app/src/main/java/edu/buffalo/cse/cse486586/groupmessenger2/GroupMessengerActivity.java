package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.KeyEvent;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.UUID;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String [] REMOTE_PORTS = {"11108", "11112", "11116", "11120","11124"};
    static final int SERVER_PORT = 10000;
    int seqNo = 0;
    int lastAccepted = 0;
    String portNumber;
    int providerSeq = 0;
//    HashMap<String, AVD> avd = new HashMap<String, AVD>();

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);


        TelephonyManager telManager = (TelephonyManager) this.getSystemService(TELEPHONY_SERVICE);
        String portString = telManager.getLine1Number().substring(telManager.getLine1Number().length() - 4);
        //TODo: verify from PA1
        portNumber = String.valueOf((Integer.parseInt(portString)) * 2);

//        AVD avd = new AVD(portNumber);


        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }


//        for(int i = 0; i<REMOTE_PORTS.length; i++) {
//            avd.put(REMOTE_PORTS[i], new AVD(REMOTE_PORTS[i], i+1));
//        }

//        System.out.println(avd.get(REMOTE_PORTS[0]));


        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        
        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));
        
        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
        final EditText editText = (EditText) findViewById(R.id.editText1);
        Button send = (Button) findViewById(R.id.button4);
        send.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String msg = editText.getText().toString() + "\n";
                Log.d("NAME_TAG", msg);
                editText.setText(""); // This is one way to reset the input box.
//                for(int i = 0; i<REMOTE_PORTS.length; i++) {
//                    Log.d("NAME_TAG", msg);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, portNumber);
//                }
            }
        });

    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            PriorityQueue<Message> queue = new PriorityQueue<Message>(25, new MessageComparator());
            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             *
             */
//            cd~https://docs.oracle.com/javase/tutorial/networking/sockets/readingWriting.html
            try {

                do {
                    Socket s = serverSocket.accept();
                    Log.d("SERVER", "Server Socket Accept");
                    BufferedReader data_in = new BufferedReader(new InputStreamReader(s.getInputStream()));

                    String message;
                    if ((message = data_in.readLine()) != null) {
                        Log.d("SERVER", message);
                        if(message.contains("PROPOSED")){
                            String[] messageDetails = message.split(":");
//                            int suggestedSeqNo = Integer.parseInt(messageDetails[1]);
                            int proposeSeqNo = (seqNo > lastAccepted ? seqNo : lastAccepted) + 1;
                            seqNo = proposeSeqNo;
                            Log.d("SERVER", "Propose Seq: "+proposeSeqNo);

                            Double seqOrigin = Double.parseDouble(proposeSeqNo+"."+messageDetails[1]);

                            //TODO: Priority Queue here

                            Message messageObject = new Message(messageDetails[0], messageDetails[1],proposeSeqNo, messageDetails[3], false, seqOrigin);
                            queue.add(messageObject);

                            PrintWriter data_out = new PrintWriter(s.getOutputStream(), true);
                            data_out.println("PROPOSAL_REPLY:"+seqOrigin);
                            data_out.close();

                        }else{
                            String[] messageDetails = message.split(":");
                            int finalSeqNo = (int) Double.parseDouble(messageDetails[1]);
                            lastAccepted = finalSeqNo;

//                            Log.d("SERVER", "FINAL SEQ: "+finalSeqNo);

                            //TODO: Priority Queue Find, Remove and then Add
                            Message msgToEdit = null;
                            Iterator<Message> queue_iter = queue.iterator();
                            while(queue_iter.hasNext()){
                                Message msg = queue_iter.next();
                                if(msg.getId().equals(messageDetails[0])){
                                    msgToEdit = msg;
                                    break;
                                }
                            }

                            Double seqOrigin = Double.parseDouble(messageDetails[1]);

                            queue.remove(msgToEdit);
                            Message messageObject = new Message(messageDetails[0], messageDetails[2], finalSeqNo, messageDetails[4], true, seqOrigin);
                            queue.add(messageObject);


                            PrintWriter data_out = new PrintWriter(s.getOutputStream(), true);
                            data_out.println("FINAL_ACK");
                            data_out.close();

                        }


                        Message top = queue.peek();
                        while (top != null && top.getDeliverable() ){
                            Message head = queue.poll();
                            Log.d("QUEUE", providerSeq+", "+head.getSequenceOrigin()+" - "+head.getMsg());
                            this.publishProgress(String.valueOf(providerSeq), head.getMsg());
                            providerSeq++;
                            top = queue.peek();
                        }


                    }
                    data_in.close();
                    s.close();
                } while (true);


            } catch (IOException e) {
                e.printStackTrace();
            }


            return null;
        }

        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[1].trim();
            TextView tv = (TextView) findViewById(R.id.textView1);
//            TextView remoteTextView = (TextView) findViewById(R.id.remote_text_display);
            tv.append(strReceived + "\t\n");

            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority("edu.buffalo.cse.cse486586.groupmessenger2.provider");
            uriBuilder.scheme("content");
            Uri mUri = uriBuilder.build();

            ContentValues cv = new ContentValues();
            cv.put("key", strings[0]);
            cv.put("value", strReceived);

            Log.d("BIPUL", strings[0]+", "+strReceived);

            getContentResolver().insert(mUri, cv);

            return;
        }
    }


    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            String msg= msgs[0];
            String portNumber = msgs[1]; //self Port
            ArrayList<Double> sequencer = new ArrayList<Double>();
            Double chosenSeqNo = 0.0;
            String uuid = UUID.randomUUID().toString();

            //sequence suggestion
            for(int i = 0; i<REMOTE_PORTS.length; i++) {
                try {
                    String remotePort = REMOTE_PORTS[i];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
//                    socket.setSoTimeout(1000);
                    /*
                     * TODO: Fill in your client code that sends out a message.
                     */
                    PrintWriter data_out = new PrintWriter(socket.getOutputStream(), true);

                    //Proposed seq No.
                    String msgToSend = uuid+":"+portNumber+":PROPOSED:"+msg;
                    Log.d("CLIENT", msgToSend);
                    data_out.println(msgToSend);
                    Log.d("CLIENT", "Socket Message sent");


                    //Received Seq No.
                    BufferedReader data_in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String message = data_in.readLine();
                    Log.d("CLIENT", "Client Socket receive "+ message);
                    if (message.contains("PROPOSAL_REPLY")){
                        Double receivedSeqNo = Double.parseDouble(message.split(":")[1]);
                        sequencer.add(receivedSeqNo);
                        socket.close();
                        Log.d("CLIENT", "Client Socket close "+receivedSeqNo);
                    }

                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException");
                }

            }

            chosenSeqNo = Collections.max(sequencer);
//            seqNo = chosenSeqNo+1;
            Log.d("CLIENT", "Non Blocking call "+chosenSeqNo);
            //------------------------------------------------------------------------------------//

            //Maximum Sequence Number
            for(int i = 0; i<REMOTE_PORTS.length; i++) {
                try {
                    String remotePort = REMOTE_PORTS[i];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
//                    socket.setSoTimeout(1000);
                    /*
                     * TODO: Fill in your client code that sends out a message.
                     */
                    PrintWriter data_out = new PrintWriter(socket.getOutputStream(), true);

                    //Proposed seq No.
                    String msgToSend = uuid+":"+chosenSeqNo+":"+portNumber+":FINAL:"+msg;
                    data_out.println(msgToSend);

                    //Received Seq No.
                    BufferedReader data_in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String message = data_in.readLine();

                    if (message.contains("FINAL_ACK")){
                        socket.close();
                        Log.d("CLIENT", "Client Socket close final ack");
                    }

                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException");
                }

            }


            return null;
        }
    }


    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }
}


class AVD {
    private String address;
    private int id;
    private int seqCount = 1;
    private Boolean alive = true;

    public AVD(String address, int id){
        this.address = address;
        this.id = id;
    }

    public String getAddress() {
        return address;
    }

    public Boolean getAlive() {
        return alive;
    }

    public void setAlive(Boolean alive) {
        this.alive = alive;
    }

    public int getId() {
        return id;
    }
}

class Message {
    private String id;
    private int sequence;
    private String origin;
    private boolean deliverable;
    private String msg;
    private Double seqOrigin;

    public Message(String id, String origin, int seq, String msg, boolean deliverable, Double seqOrigin){
        this.id = id;
        this.msg = msg;
        this.origin = origin;
        this.sequence = seq;
        this.deliverable = deliverable;
        this.seqOrigin = seqOrigin;
    }

    public String getMsg() { return msg; }

    public boolean getDeliverable() { return deliverable; }

    public String getId() {
        return id;
    }

    public Double getSequenceOrigin(){
        return Double.parseDouble(this.sequence+"."+this.origin);
    }

    public Double getSeqOrigin() {
        return seqOrigin;
    }
}

class MessageComparator implements Comparator<Message>{

    @Override
    public int compare(Message lhs, Message rhs) {
        if(lhs.getSeqOrigin() < rhs.getSeqOrigin()) {
            return -1;
        }else{
            return 1;
        }
    }
}
