package com.example.demo;

import com.example.pastry.past.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicLong;

import com.example.pastry.past.PastTutorial;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import rice.p2p.past.PastContent;

@RestController
public class GreetingController implements CommandLineRunner {

    private static final String template = "%s";
    private final AtomicLong counter = new AtomicLong();

    private int bindport;
    private InetAddress bootaddr;
    private int bootport;
    private int numNodes;
    private boolean isMemory;
    private int replicationFactor;
    PastTutorial app;

    @Override
    public void run(String... args) throws Exception {

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

    public GreetingController() throws Exception {
    }

    @GetMapping("/greeting")
    public Greeting greeting(@RequestParam(value = "name", defaultValue = "World") String name) {
        return new Greeting(counter.incrementAndGet(), String.format(template, name));
    }

    @GetMapping("/get")
    public Greeting get(@RequestParam(value="key") String key) throws InterruptedException {
        MyPastContent result = (MyPastContent) app.getMethod(key);
        return new Greeting(counter.incrementAndGet(), String.format(template, result.content));
    }

    @GetMapping("/put")
    public Greeting put(@RequestParam(value="key") String key, @RequestParam(value="value") String value){
        long t0 = System.nanoTime();
        app.putMethod(key, value);
        long t1 = System.nanoTime();
//        System.out.println("Nano time is: " + String.valueOf(t1-t0));
        return new Greeting(counter.incrementAndGet(), String.format(template, key));
    }


}