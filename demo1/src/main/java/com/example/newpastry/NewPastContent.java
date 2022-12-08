//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.example.newpastry;

import java.io.Serializable;
import rice.p2p.commonapi.Id;
import rice.p2p.past.PastContentHandle;
import rice.p2p.past.PastException;

public interface NewPastContent extends Serializable {
    NewPastContent checkInsert(Id var1, NewPastContent var2) throws PastException;

    PastContentHandle getHandle(NewPast var1);

    Id getId();

    boolean isMutable();
}
