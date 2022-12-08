package com.example.pastry.past;

import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.NodeHandle;

public class IdObject {

    private Id id;
    private NodeHandle nh;

    public IdObject(Id id, NodeHandle nh){
        this.id = id;
        this.nh = nh;
    }

    public Id getId() {
        return id;
    }

    public void setId(Id id) {
        this.id = id;
    }

    public NodeHandle getNh() {
        return nh;
    }

    public void setNh(NodeHandle nh) {
        this.nh = nh;
    }

    @Override
    public String toString() {
        return "IdObject{" +
                "id=" + id +
                ", nh=" + nh +
                '}';
    }
}
