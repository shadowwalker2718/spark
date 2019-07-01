package org.apache.spark.examples.sql.Kryo;

import java.io.Serializable;

import org.apache.spark.serializer.KryoRegistrator;

import com.esotericsoftware.kryo.Kryo;

public class CustomizedKryoRegistrator implements KryoRegistrator, Serializable{
    @Override
    public void registerClasses(Kryo kryo) {
        kryo.register(SampleBean.class);
    }
}