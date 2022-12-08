package com.example.demo;

import com.example.pastry.past.MyPastContent;
import com.example.pastry.past.PastTutorial;
import org.springframework.boot.CommandLineRunner;
import org.springframework.web.bind.annotation.RequestParam;
import rice.p2p.util.tuples.Tuple;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

public class GreetingTcpServer{

    private static final String template = "%s";
    private final AtomicLong counter = new AtomicLong();

    private int bindport;
    private InetAddress bootaddr;
    private int bootport;
    private int numNodes;
    private boolean isMemory;
    private int replicationFactor;
    PastTutorial app;
    final int serverPort = 8080;

    public GreetingTcpServer(String[] args) throws Exception {
        this.bindport = Integer.parseInt(args[0]);
        this.bootaddr = InetAddress.getByName(args[1]);
        this.bootport = Integer.parseInt(args[2]);
        this.numNodes = Integer.parseInt(args[3]);
        this.isMemory = Boolean.parseBoolean(args[4]);
        this.replicationFactor = Integer.parseInt(args[5]);
        InetSocketAddress bootaddress = new InetSocketAddress(bootaddr, bootport);

        app = new PastTutorial(this.bindport, bootaddress, this.bootport, this.numNodes, this.isMemory, this.replicationFactor);
        System.out.println("" + this.bindport +  this.bootaddr + this.bootport + this.numNodes + this.isMemory + this.replicationFactor);
    }

    public void startServer() throws IOException, ExecutionException, InterruptedException {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(25);

        ServerSocket serverSocket = new ServerSocket(this.serverPort);

        boolean running = true;
        Socket socket;

        while(running){
            socket = serverSocket.accept();

            Runnable task = new RequestHandler(socket, this.app);
            System.out.println(task.toString());
            executor.submit(task);

            System.out.println(executor.getTaskCount());

        }
    }
}

class RequestHandler implements Runnable{

    private Socket clientSocket;
    private InetAddress clientAddr;
    private int clientPort;
    private PastTutorial app;

    @Override
    public String toString() {
        return "RequestHandler{" +
                "clientSocket=" + clientSocket +
                ", clientAddr=" + clientAddr +
                ", clientPort=" + clientPort +
                ", app=" + app +
                '}';
    }

    public RequestHandler(Socket clientSocket, PastTutorial app){
        this.clientSocket = clientSocket;
        this.clientAddr = this.clientSocket.getInetAddress();
        this.clientPort = this.clientSocket.getPort();
        this.app = app;
    }
    @Override
    public void run() {
        try
        {

            System.out.println( "Received a connection" );

            // Write out our header to the client
            DataInputStream in = new DataInputStream(this.clientSocket.getInputStream());

            DataOutputStream out = new DataOutputStream(this.clientSocket.getOutputStream());

            String line = "";

            // reads message from client until "Over" is sent
            while (!line.equals("over"))
            {
                try
                {
                    line = in.readUTF();
                    if(line.equals("1")){
                        out.writeUTF("Echo");
                        continue;
                    }
                    String[] temp = line.split(" ");
//                    System.out.println(temp[0] + " " + temp.length + " " + this.clientAddr);
//                    System.out.println("" + temp[0] + " " + temp.length);
                    if(line.length() > 0 && !line.equals("over")){
                        if(temp.length == 1){
                            MyPastContent result = (MyPastContent) app.getMethod(temp[0]);
                            try {
                                out.writeUTF(result.content);
                            }
                            catch(NullPointerException e){
                                e.printStackTrace();
                            }
                        }
                        else{
                            app.putMethod(temp[0], temp[1]);
//                            System.out.println("Trues for " + temp[0]);
                            out.writeUTF("Trues");
                        }
                    }
                }
                catch(IOException i)
                {
                    System.out.println("Call failed for client " + this.clientAddr + " listening on port " + this.clientPort);
                    break;
                }
            }
            System.out.println("Closing connection for "+this.clientSocket.getInetAddress() + " " + this.clientSocket.getPort());

            // close connection
            this.clientSocket.close();
            in.close();
            out.close();

            System.out.println( "Connection closed" );
        }
        catch( Exception e )
        {
            e.printStackTrace();
        }
    }
}
