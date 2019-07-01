package org.apache.spark.examples.sql.kryo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.*;
import org.apache.spark.api.java.*;
import java.util.*;

public class TestKryoSerialization {
    public static void main(String[] args) {
        final SparkSession ss = SparkSession.builder().appName("demo")
                .master("local")
                .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                .config("spark.kryo.registrator","org.apache.spark.examples.sql.kryo.CustomizedKryoRegistrator")
                .enableHiveSupport().getOrCreate();

        ss.sparkContext().setLogLevel("ERROR");

        final Map<String, Double> m = new HashMap<>();
        m.put("x", 12.34);

        final SampleBean sampleBean = new SampleBean();
        sampleBean.setAMap(m);

        final List<SampleBean> bs = new ArrayList<>();
        bs.add(sampleBean);

        final Dataset<SampleBean> ds = ss.createDataset(bs,  Encoders.bean(SampleBean.class));
        final JavaRDD<SampleBean> rdd = ds.javaRDD();
        final List<SampleBean> r = rdd.collect();
        System.out.println(r.size()==1);
        try {
            Thread.sleep(90000);
        }catch (Exception e){}
        ss.close();
    }
}
