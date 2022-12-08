package com.example.storage.datanodes;

import com.example.storage.MemoryTyp;

public abstract class DataNodes {
    long space = 15000;
    MemoryTyp type = MemoryTyp.MEMORY;
}

class MemoryDataNodeImpl extends DataNodes{

}
