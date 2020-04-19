package com.travel.hbase;

import com.travel.common.Constants;
import com.travel.utils.HbaseTools;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class CreateHbaseTableInit {
    public static void main(String[] args) throws IOException {

        Connection hbaseConn = HbaseTools.getHbaseConn();
        //创建四张表

        String[] tableNames = {"order_info", "renter_info", "driver_info", "opt_alliance_business"};

        Admin admin = hbaseConn.getAdmin();


         /*  //HBase自带的分区工具类，自动帮我们进行分区
            //获取到的是16进制的字符串
            RegionSplitter.HexStringSplit spliter = new RegionSplitter.HexStringSplit();
            byte[][] split = spliter.split(8);
            //适合rowkey经过hash或者md5之后的字符串
            RegionSplitter.UniformSplit uniformSplit = new RegionSplitter.UniformSplit();
            byte[][] split1 = uniformSplit.split(8);*/


        //通过字节数组来指定预分区的规则
        byte[][] bytes = new byte[8][];
        for(int i = 0;i<8;i ++){
            //  0001|    0002|   0003|
            //左补全  0000|  0001   0002
            String leftPad = StringUtils.leftPad(i + "", 4, "0");
            bytes[i] = Bytes.toBytes(leftPad + "|");

        }

        for(String tableName:tableNames){
            if(!admin.tableExists(TableName.valueOf(tableName))){

                HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(Constants.DEFAULT_DB_FAMILY);
                hTableDescriptor.addFamily(hColumnDescriptor);
                admin.createTable(hTableDescriptor,bytes);
            }
        }
        admin.close();
    }

}
