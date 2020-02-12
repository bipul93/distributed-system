package edu.buffalo.cse.cse486586.groupmessenger1;

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
import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.List;

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

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);


        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }



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
                editText.setText(""); // This is one way to reset the input box.
                for(int i = 0; i<REMOTE_PORTS.length; i++) {
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msg, REMOTE_PORTS[i]);
                }
            }
        });

    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             *
             */
//            cd~https://docs.oracle.com/javase/tutorial/networking/sockets/readingWriting.html
            try {

                do {
                    Socket s = serverSocket.accept();
                    Log.d("NAME_TAG", "Server Socket Accept");
                    BufferedReader data_in = new BufferedReader(new InputStreamReader(s.getInputStream()));
                    String message;
                    if ((message = data_in.readLine()) != null) {

                        Log.d("BIPUL", seqNo+", "+message);
                        this.publishProgress(new String[] {String.valueOf(seqNo), message});
                        seqNo++;

                        PrintWriter data_out = new PrintWriter(s.getOutputStream(), true);
                        data_out.println("close");
                        data_out.close();
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
            uriBuilder.authority("edu.buffalo.cse.cse486586.groupmessenger1.provider");
            uriBuilder.scheme("content");
            Uri mUri = uriBuilder.build();

            ContentValues cv = new ContentValues();
            cv.put("key", strings[0]);
            cv.put("value", strReceived);

            getContentResolver().insert(mUri, cv);


            return;
        }
    }


    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                String msgToSend = msgs[0];
                String remotePort = msgs[1];

                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));

                /*
                 * TODO: Fill in your client code that sends out a message.
                 */
                Log.d("NAME_TAG", msgToSend);
                PrintWriter data_out = new PrintWriter(socket.getOutputStream(), true);
                data_out.println(msgToSend);
                Log.d("NAME_TAG", "Socket Message sent");

                BufferedReader data_in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String message = data_in.readLine();
                Log.d("NAME_TAG", "Client Socket receive"+ message);
                if (message.equalsIgnoreCase("close")){
                    socket.close();
                    Log.d("NAME_TAG", "Client Socket close");
                }

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
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
