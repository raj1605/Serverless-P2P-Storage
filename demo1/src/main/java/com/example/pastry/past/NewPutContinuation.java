package com.example.pastry.past;

import rice.Continuation;
import rice.p2p.past.PastContent;

import java.util.Arrays;

public class NewPutContinuation implements Continuation<Boolean[], Exception> {
    private Boolean[] boolArr;



    @Override
    public void receiveResult(Boolean[] booleans) {
        this.boolArr = booleans;
    }

    @Override
    public void receiveException(Exception e) {

    }

    @Override
    public String toString() {
        return "NewPutContinuation{" +
                "numOfSuccess=" + boolArr.toString()+
                '}';
    }

    public Boolean[] getNumOfSuccess(){
        return(this.boolArr);
    }
}
