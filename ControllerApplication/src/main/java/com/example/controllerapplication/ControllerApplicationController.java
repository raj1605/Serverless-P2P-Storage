package com.example.controllerapplication;

import com.example.service.RegisterService;
import com.example.storage.ControllerStorage;
import com.example.storage.MemoryTyp;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.Queue;

@RestController
public class ControllerApplicationController {

    RegisterService registerService;
    ControllerStorage controllerStorage;

    public ControllerApplicationController(){
        controllerStorage = ControllerStorage.getInstance();
        registerService = new RegisterService(controllerStorage);
    }

    @GetMapping(path="/test")
    public String chumma(){
        return("Hello World");
    }
    @GetMapping(path = "/register")
    public Connection registerApplication(
            @RequestParam(value="jobName") String jobName, @RequestParam(value="type") String type,
            @RequestParam(value="space") String space)
            throws HttpMessageNotReadableException {

        Hint hint;
        if(type.equalsIgnoreCase("Memory")) {
            hint = new Hint(MemoryTyp.MEMORY, Long.parseLong(space));
        }
        else{
            hint = new Hint(MemoryTyp.PERSISTENT, Long.parseLong(space));
        }
        String nodeIp = registerService.registerJob(jobName, this.controllerStorage, hint);
        Connection conn = new Connection(nodeIp);
        Queue<String> q = registerService.getListOfNodes();

        String[] arr = new String[q.size()-1];
        q.remove(nodeIp);
        q.toArray(arr);
        System.out.println(q.toString());
        conn.setOtherIps(arr);

        return conn;
    }

}
