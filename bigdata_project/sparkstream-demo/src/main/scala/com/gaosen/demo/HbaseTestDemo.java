package com.gaosen.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import javax.sound.midi.Soundbank;
import java.io.IOException;

public class HbaseTestDemo {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum","10.201.0.208");
        try{
            Connection connection = ConnectionFactory.createConnection(conf);
            System.out.println("连接测试通过!");
        } catch (Exception e){
            e.printStackTrace();
        }

        Connection conn = ConnectionFactory.createConnection(conf);
        Table gadaite = conn.getTable(TableName.valueOf("app:data_event_v2.0"));
        ResultScanner scanner = gadaite.getScanner(new Scan());
        for (Result sc:scanner){
            for(Cell c:sc.rawCells()){
                System.out.println(c);
            }
        }
    }

}
