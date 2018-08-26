package com.mr.research.unsupervised.k_means.Utils;

import java.util.Map;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Configuration_settings {

    @JsonProperty
    public Map<String,String> hdfs;

    public void setHdfs(Map<String, String> hdfs) {
        this.hdfs = hdfs;
    }

    public Map<String, String> getHdfs() {
        return hdfs;
    }
}
