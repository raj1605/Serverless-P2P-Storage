package com.example.storage;

import java.util.UUID;

public class Resource {

    private String id;
    private String dataNodeIp;
    private MemoryTyp type;
    private long numberOfConnections;
    private long totalSpace;
    private long spaceLeft;

    public Resource(String dataNodeIp, MemoryTyp type, long totalSpace){
        this.id = UUID.randomUUID().toString();
        this.dataNodeIp = dataNodeIp;
        this.type = type;
        this.totalSpace = totalSpace;
        this.spaceLeft = this.totalSpace;
        this.numberOfConnections = 0;
    }

    public long getTotalSpace() {
        return totalSpace;
    }

    public void setTotalSpace(long totalSpace) {
        this.totalSpace = totalSpace;
    }

    public long getSpaceLeft() {
        return spaceLeft;
    }

    public void setSpaceLeft(long spaceLeft) {
        this.spaceLeft = spaceLeft;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDataNodeIp() {
        return dataNodeIp;
    }

    public void setDataNodeIp(String dataNodeIp) {
        this.dataNodeIp = dataNodeIp;
    }

    public MemoryTyp getType() {
        return type;
    }

    public void setType(MemoryTyp type) {
        this.type = type;
    }

    public long getNumberOfConnections() {
        return numberOfConnections;
    }

    public void setNumberOfConnections(long numberOfConnections) {
        this.numberOfConnections = numberOfConnections;
    }
}
