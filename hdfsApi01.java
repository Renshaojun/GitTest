package com.rsj.hdfsApi;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

/**
 * HDFS API测试用例
 */
public class hdfsApi01 {

    FileSystem fileSystem;

    @Before
    public  void setUp() throws Exception {
        //程序入口
        Configuration conf = new Configuration();
        conf.set("dfs.replication","1");
        URI uri = new URI("hdfs://hadoop01:9000");
        //获取hdfs客户端对象
        fileSystem = FileSystem.get(uri, conf, "rsj");
    }

    @After
    public  void tearDown() throws  Exception{
        if (null != fileSystem){
           fileSystem.close();
        }
    }

    //hdfs文件重命名
    @Test
    public void rename() throws Exception{
       Path src = new Path("/user/rsj/test/b.txt");
       Path dst = new Path("/user/rsj/test/c.txt");
       fileSystem.rename(src,dst);
    }

    //hdfs指定读取块大小，合并

    //有问题,下载的还是整个文件的大小,buffSize的大小默认为4096
    @Test
    public void downLoad1() throws Exception{
        FSDataInputStream in = fileSystem.open(new Path("/user/rsj/test/jdk-8u45-linux-x64.gz"));
        FileOutputStream out  = new FileOutputStream(new File("out/jdk.tgz.part0"));
        //0-128MB
        IOUtils.copyBytes(in,out,1024*128*1024,false);
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
    }

    @Test
    public void downLoad3() throws Exception{
        FSDataInputStream in = fileSystem.open(new Path("/user/rsj/test/jdk-8u45-linux-x64.gz"));
        FileOutputStream out  = new FileOutputStream(new File("out/jdk.tgz.part3"));
        //0-128MB
       byte[] buf= new byte[1024];//1kb
       for (int i =0;i < 1024 *128;i++){
           in.read(buf);
           out.write(buf);
       }
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
    }


    @Test
    public void downLoad2() throws Exception{
        FSDataInputStream in = fileSystem.open(new Path("/user/rsj/test/jdk-8u45-linux-x64.gz"));
        FileOutputStream out  = new FileOutputStream(new File("out/jdk.tgz.part1"));
        //seek 128M
        in.seek(1024*128*1024);
        IOUtils.copyBytes(in,out,fileSystem.getConf());
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
    }



    //hdfs读取本地文件，默认块大小
    @Test
    public void copyFromLocal() throws Exception{
        BufferedInputStream in = new BufferedInputStream(new FileInputStream(new File("data/a.txt")));
        FSDataOutputStream out = fileSystem.create(new Path("/test/a_out4.txt"));
        IOUtils.copyBytes(in,out,4096);
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);

    }

}
