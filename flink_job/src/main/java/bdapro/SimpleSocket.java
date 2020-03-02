package bdapro;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;

public class SimpleSocket {

    //static ServerSocketNAKACHKATA variable
    private static ServerSocket server;
    //socket server port on which it will listen


    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        String a = "11,22,FOOD,44,0.9\n";
        String b = "11,22,ELECTRONICS,44,0.9\n";
        String click = "11,22,ELECTRONICS,44\n";
        //create the socket server object
        Long.parseLong("11");
        server = new ServerSocket(      7777);
        //keep listens indefinitely until receives 'exit' call or program terminates
        System.out.println("Waiting for the client request");
        //creating socket and waiting for client connection
        Socket socket = server.accept();
        //read from socket to ObjectInputStream object
        DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());

        boolean loop_condition = true;
        while(loop_condition){

            System.out.println("Sending string to the ServerSocket");
            int rand = new Random().nextInt(3);
            String send = "";
            if(rand==0){
                send = a;
            }else if(rand==1){
                send = b;
            }else {
                send = click;
            }
            // write the message we want to send
            dataOutputStream.writeUTF(send);
            dataOutputStream.flush(); // send the message
            Thread.sleep(100);
        }
        System.out.println("Shutting down Socket server!!");
        //close the ServerSocketNAKACHKATA object
        server.close();
    }

}
