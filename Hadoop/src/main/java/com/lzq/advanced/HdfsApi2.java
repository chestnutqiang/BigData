package com.lzq.advanced;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

/** 利用HDFS java API操作文件 */
public class HdfsApi2 {
    /*
     * 通过FileSystem API做read操作， 设置位置信息seek()
     * 总结：FSDataInputStream:可执行Seekable(),
     * 而FSDataOutputStream没有，
     */
    @Test
    public void seekByAPI() throws IOException {

        /*
          "fs.defaultFS":"hdfs://ns",
          "dfs.nameservices":"ns",
          "dfs.ha.namenodes.ns":"nn1,nn2",
          "dfs.namenode.rpc-address.ns.nn1":"node1:9000",
          "dfs.namenode.rpc-address.ns.nn2":"node2:9000",
          "dfs.client.failover.proxy.provider.ns": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
         */
        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", "hdfs://ns");
        //conf.set("dfs.namenode.http-address.ns.nn1", "node1:50070");
        //conf.set("dfs.namenode.http-address.ns.nn2", "node2:50070");
        conf.set("dfs.nameservices", "ns");
        conf.set("dfs.ha.namenodes.ns", "nn1,nn2");
        conf.set("hadoop.user.name", "admin");
        conf.set("dfs.namenode.rpc-address.ns.nn2", "node2:9000");
        conf.set("dfs.namenode.rpc-address.ns.nn1", "node1:9000");
        conf.set(
                "dfs.client.failover.proxy.provider.ns",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

        //conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        FileSystem fs = FileSystem.get(conf);
        Path file = new Path("/no_crypto/a.txt");
        FSDataInputStream in = fs.open(file);
        //IOUtils.copyBytes(in, System.out, 4096, false); // 注意：流不能关闭
        in.seek(1); // 使输入指针回到0，相当于又读了一遍到控制台
        IOUtils.copyBytes(in, System.out, 4096, false);
        in.seek(2);
        IOUtils.copyBytes(in, System.out, 4096, true);

        in.close();
        fs.close();

    }


    @Test
    public void seekByAPIMultiThread() throws IOException {

        /*
          "fs.defaultFS":"hdfs://ns",
          "dfs.nameservices":"ns",
          "dfs.ha.namenodes.ns":"nn1,nn2",
          "dfs.namenode.rpc-address.ns.nn1":"node1:9000",
          "dfs.namenode.rpc-address.ns.nn2":"node2:9000",
          "dfs.client.failover.proxy.provider.ns": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
         */
        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", "hdfs://ns");
        conf.set("dfs.nameservices", "ns");
        conf.set("dfs.ha.namenodes.ns", "nn1,nn2");
        conf.set("hadoop.user.name", "admin");
        conf.set("dfs.namenode.rpc-address.ns.nn2", "node2:9000");
        conf.set("dfs.namenode.rpc-address.ns.nn1", "node1:9000");
        conf.set(
                "dfs.client.failover.proxy.provider.ns",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

        FileSystem fs = FileSystem.get(conf);
        Path file = new Path("/no_crypto/a.txt");
        //FSDataInputStream in = fs.open(file);


        Thread thread1 = new Thread(
                () -> {
                    FSDataInputStream in = null;
                    try {
                        System.out.println("============== Thread 1 ==================");
                        in = fs.open(file);
                        in.seek(0);
                        IOUtils.copyBytes(in, System.out, 10L, false);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                }
        );
        thread1.start();

        Thread thread2 =
                new Thread(
                        () -> {
                            FSDataInputStream in = null;
                            try {
                                System.out.println("============== Thread 2 ==================");
                                in = fs.open(file);
                                in.seek(5);
                                IOUtils.copyBytes(in, System.out, 10L, false);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
        thread2.start();

        try {
            thread1.join();
            thread2.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }
}
