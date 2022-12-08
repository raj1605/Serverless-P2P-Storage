package com.example.storage;

import com.example.controllerapplication.Hint;

import java.util.*;

public class ControllerStorage {

    public Map<MemoryTyp, Long> totalSpaceAvailable;
    public Map<MemoryTyp, Long> totalSpaceOccupied;
    public Map<String, Resource> resources;
    public Queue<Resource> resourcePriorityQueue;
    public Map<String, String> jobResourceMap;
    public Map<String, JobData> jobDataMap;
    private static ControllerStorage controllerStorageInstance;

    private ControllerStorage(){
        totalSpaceAvailable = new HashMap<>();
        totalSpaceAvailable.put(MemoryTyp.MEMORY, 0l);
        totalSpaceAvailable.put(MemoryTyp.PERSISTENT, 0l);

        totalSpaceOccupied = new HashMap<>();
        totalSpaceOccupied.put(MemoryTyp.MEMORY, 0l);
        totalSpaceOccupied.put(MemoryTyp.PERSISTENT, 0l);

        resources = new HashMap<>();
        jobResourceMap = new HashMap<>();
        jobDataMap = new HashMap<>();

        Comparator<Resource> resourceSpaceVailableComparator = new Comparator<Resource>() {
            @Override
            public int compare(Resource o1, Resource o2) {
                return (int) (o1.getNumberOfConnections() - o2.getNumberOfConnections());
            }
        };
        resourcePriorityQueue = new PriorityQueue<>(resourceSpaceVailableComparator);
    }

    public static ControllerStorage getInstance(){
        if(controllerStorageInstance == null){
            controllerStorageInstance = new ControllerStorage();
        }

        return(controllerStorageInstance);
    }

    public String addJob(String jobName, Hint hint){
        JobData jobData = new JobData(jobName, hint);
        String jobId = jobData.getJobId();
        jobDataMap.put(jobId, jobData);
        return(jobId);
    }

    public String addResource(Resource resource){
        resources.put(resource.getId(), resource);
        resourcePriorityQueue.add(resource);
        this.totalSpaceAvailable.put(resource.getType(), this.totalSpaceAvailable.get(resource.getType()) + resource.getTotalSpace());
        return(resource.getId());
    }

    public Resource getResourceForUsage(Hint hint){
        this.totalSpaceOccupied.put(hint.getType(), this.totalSpaceOccupied.get(hint.getType()) + hint.getSpace());
        this.totalSpaceAvailable.put(hint.getType(), this.totalSpaceAvailable.get(hint.getType()) - hint.getSpace());

        Resource resource = resourcePriorityQueue.remove();
        resource.setNumberOfConnections(resource.getNumberOfConnections() + 1);
        resourcePriorityQueue.add(resource);
        return resource;
    }

    public boolean isSpaceAvailable(Hint hint) {
        if(0.8 * (this.totalSpaceOccupied.get(hint.getType()) + this.totalSpaceAvailable.get(hint.getType())) >=
                (hint.getSpace() + this.totalSpaceOccupied.get(hint.getType()))
                && this.totalSpaceAvailable.get(hint.getType()) >= hint.getSpace()){
//        if(0.8 * this.totalSpaceAvailable.get(hint.getType()) >= hint.getSpace() + this.totalSpaceOccupied.get(hint.getType())){
            return(true);
        }
        else{
            return(false);
        }
    }
}
