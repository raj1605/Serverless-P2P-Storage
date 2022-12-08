package com.example.pastry.past;

import com.example.newpastry.NewPastImpl;
import rice.Continuation;
import rice.p2p.past.PastContent;

public class NewContinuation implements Continuation<PastContent, Exception> {
    private PastContent pastContent;
    @Override
    public void receiveResult(PastContent o) {
        this.pastContent = o;
    }

    @Override
    public void receiveException(Exception e) {

    }

    @Override
    public String toString() {
        return "NewContinuation{" +
                "pastContent=" + pastContent +
                '}';
    }

    public PastContent getPastContent(){
        return(this.pastContent);
    }
}
