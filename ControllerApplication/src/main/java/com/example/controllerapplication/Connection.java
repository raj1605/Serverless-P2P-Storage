package com.example.controllerapplication;

public class Connection {

    private String ip;
    private String[] otherIps;

    public String[] getOtherIps() {
        return otherIps;
    }

    public void setOtherIps(String[] otherIps) {
        this.otherIps = otherIps;
    }

    public Connection(String ip){
        this.ip = ip;
    }

    public void setIp(String ip){
        this.ip = ip;
    }

    public String getIp(){
        return(this.ip);
    }

    @Override
    public String toString() {
        return "Connection{" +
                "ip='" + ip + '\'' +
                '}';
    }
}
