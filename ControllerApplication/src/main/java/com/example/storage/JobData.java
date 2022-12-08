package com.example.storage;

import com.example.controllerapplication.Hint;

import java.util.UUID;

public class JobData {
    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public Hint getHint() {
        return hint;
    }

    public void setHint(Hint hint) {
        this.hint = hint;
    }

    private String jobId;
    private String jobName;
    private Hint hint;

    public JobData(String jobName, Hint hint){
        this.jobId = UUID.randomUUID().toString();
        this.jobName = jobName;
        this.hint = hint;
    }

}
