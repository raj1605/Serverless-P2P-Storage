package com.example.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class Demo1Application {

    public static void main(String[] args) throws Exception{
        if(args[args.length - 1].equalsIgnoreCase("tcp")){
            GreetingTcpServer tcpServer = new GreetingTcpServer(args);
            tcpServer.startServer();
        }
        else{
            SpringApplication.run(Demo1Application.class, args);
        }

    }

}
