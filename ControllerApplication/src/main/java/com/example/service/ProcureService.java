package com.example.service;

import com.jcraft.jsch.*;

import java.io.IOException;
import java.io.InputStream;

class ProcureService {

    private String hostIp;
    private JSch jsch;
    private String userName;
    private String privateKeyPath;
    private int replicationFactor;
    public ProcureService(String hostIp, int replicationFactor){
        this.hostIp = hostIp;
        this.replicationFactor = replicationFactor;
        System.out.println(this.hostIp + " ---< host ip");
        jsch = new JSch();
        this.userName = "Raj1605";
        System.out.println(System.getProperty("os.name"));
        if(System.getProperty("os.name") != "Windows 10")
            this.privateKeyPath = "C:/Users/Monish/.ssh/id_ed25519";
        else
            this.privateKeyPath = "/users/Raj1605/.ssh/id_rsa";
    }

    public boolean procureHost(){
        try {
            if(System.getProperty("os.name") == "Windows 10")
                jsch.setKnownHosts("C:/Users/Monish/.ssh/known_hosts");
//            String privateKeyPath = "C:/Users/Monish/.ssh/id_ed25519";
            jsch.addIdentity(privateKeyPath, "Monish123!@#");
            String username = "Raj1605";

            Session session = jsch.getSession(username, this.hostIp, 22);
            session.setConfig("PreferredAuthentications", "publickey,keyboard-interactive,password");
            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect();

            Channel channel = session.openChannel("exec");
            String cmd = "docker run -p 8080:8080 raj1605/project-warehouse:tcp 9001 172.17.0.2 9001 5 true ";
            cmd += replicationFactor;
            ((ChannelExec) channel).setCommand(cmd);
            channel.setInputStream(null);
            ((ChannelExec) channel).setErrStream(System.err);

            InputStream in = channel.getInputStream();
            channel.connect();
            channel.disconnect();
            session.disconnect();
            System.out.println("DONE");
        }
        catch(Exception e){
            e.printStackTrace();
            return(false);
        }
        return(true);
    }

//    public static void main(String[] args){
//        ProcureService ps = new ProcureService("amd177.utah.cloudlab.us");
//        ps.procureHost();
//    }
}

