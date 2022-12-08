package com.example.controllerapplication;

import com.example.storage.MemoryTyp;
import org.springframework.beans.factory.annotation.Autowired;

public class Hint {

    @Autowired
    private MemoryTyp type;
    @Autowired
    private long space;
    private int replicationFactor;

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public MemoryTyp getType() {
        return type;
    }

    public void setType(MemoryTyp type) {
        this.type = type;
    }

    public long getSpace() {
        return space;
    }

    public void setSpace(long space) {
        this.space = space;
    }

    public Hint(MemoryTyp type, long space){
        this.type = type;
        this.space = space;
    }

    @Override
    public String toString() {
        return "Hint{" +
                "type='" + type + '\'' +
                ", space='" + space + '\'' +
                '}';
    }
}
