package com.example.service;

import com.example.controllerapplication.Hint;
import com.example.exceptions.HostNotProcuredException;
import com.example.storage.ControllerStorage;
import com.example.storage.MemoryTyp;
import com.example.storage.Resource;

import javax.naming.ldap.Control;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Scanner;

public class RegisterService {
    private ControllerStorage controllerStorage;
    private Queue<String> listOfNodes;
    private final long  memoryNodeSize = 10000;
    private final long  persistentNodeSize = 10000;

    public RegisterService(ControllerStorage controllerStorage){
        this.controllerStorage = controllerStorage;
        this.listOfNodes = new PriorityQueue<String>();

        this.listOfNodes.add("hp099.utah.cloudlab.us");
        this.listOfNodes.add("hp099.utah.cloudlab.us");
        this.listOfNodes.add("hp099.utah.cloudlab.us");


        //Populate all the IP addresses into the priority queue
        try {
            File Obj = new File("/users/Raj1605/hostlists.txt");
            Scanner Reader = new Scanner(Obj);
            while (Reader.hasNextLine()) {
                String data = Reader.nextLine();
                this.listOfNodes.add(data);
            }
            Reader.close();
        }
        catch (FileNotFoundException e) {
            System.out.println("An error has while reading the hosts.txt file");
            e.printStackTrace();
        }
    }

    public Queue<String> getListOfNodes() {
        return listOfNodes;
    }

    public String registerJob(String jobName, ControllerStorage controllerStorage){
        Hint hint = populateDefaultHint();
        return this.registerJob(jobName, controllerStorage, hint);
    }

    public String registerJob(String jobName, ControllerStorage controllerStorage, Hint hint){
        String jobId = controllerStorage.addJob(jobName, hint);
        String resourceId;
        System.out.println("In register service...");
        System.out.println("Job name: " + jobName + " Hints: " + hint);
        System.out.println("Total space available: " + controllerStorage.totalSpaceAvailable);
        System.out.println("Total space occupied " + controllerStorage.totalSpaceOccupied);
        if(!controllerStorage.isSpaceAvailable(hint)){
            System.out.println("Need to add resource. Now adding");
            resourceId = this.addResources(jobId, hint, controllerStorage);
        }
        else{
            System.out.println("No need to add resource");
            resourceId = controllerStorage.getResourceForUsage(hint).getId();
        }
        System.out.println("Total space available: " + controllerStorage.totalSpaceAvailable);
        System.out.println("Total space occupied " + controllerStorage.totalSpaceOccupied);

        return(controllerStorage.resources.get(resourceId).getDataNodeIp());

    }

    public boolean procureHost(String ip, int replicationFactor) throws HostNotProcuredException {
        ProcureService procureService = new ProcureService(ip, replicationFactor);
        return procureService.procureHost();
//        return(true);
    }

    public String addResources(String jobId, Hint hint, ControllerStorage controllerStorage){
        String hostIp;
        Resource resource;
        try{
            hostIp = (String) this.listOfNodes.remove();
            if(procureHost(hostIp, hint.getReplicationFactor())){
                if(hint.getType() == MemoryTyp.MEMORY)
                    resource = new Resource(hostIp, hint.getType(), this.memoryNodeSize);
                else
                    resource = new Resource(hostIp, hint.getType(), this.persistentNodeSize);
                controllerStorage.totalSpaceOccupied.put(hint.getType(), controllerStorage.totalSpaceOccupied.get(hint.getType())
                        + hint.getSpace());
                controllerStorage.totalSpaceAvailable.put(hint.getType(), controllerStorage.totalSpaceAvailable.get(hint.getType())
                        - hint.getSpace());
                return controllerStorage.addResource(resource);
            }
        }
        catch(NoSuchElementException e){
            System.out.println("No hosts left to procure");
            e.printStackTrace();
        } catch (HostNotProcuredException e) {
            throw new RuntimeException(e);
        }

        return "";

    }
    private Hint populateDefaultHint() {
        Hint tempHint = new Hint(MemoryTyp.MEMORY, 1000 );
        return(tempHint);
    }

}
