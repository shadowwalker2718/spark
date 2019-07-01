package org.apache.spark.examples.sql.Kryo;

import java.io.Serializable;
import java.util.Map;

public final class SampleBean implements Serializable{
    private Map<String,Double> aMap;
    public Map<String,Double> getAMap(){
        return aMap;
    }
    public void setAMap(Map<String,Double> aMap) {
        this.aMap = aMap;
    }
}