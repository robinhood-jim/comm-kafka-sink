package com.robin.etl.datawriter;

import com.robin.etl.util.datasink.CassandraPressitUtil;
import org.apache.avro.Schema;

import java.util.Map;

/**
 * <p>Project:  kafka-sink</p>
 * <p>
 * <p>Description:com.robin.etl.datawriter</p>
 * <p>
 * <p>Copyright: Copyright (c) 2019 create at 2019年03月07日</p>
 * <p>
 * <p>Company: zhcx_DEV</p>
 *
 * @author robinjim
 * @version 1.0
 */
public class CassandraDataWriter extends AbstractDBLikeDataWriter {
    private CassandraPressitUtil util;
    private String clusterName;
    private String userName;
    private String password;
    private String executeSql;
    private String[] insertColumns;

    public CassandraDataWriter(Schema schema, Map<String, Object> paramCfgMap)
    {
        super(schema, paramCfgMap);
        clusterName=paramCfgMap.get("target.clusterName").toString();
        if(paramCfgMap.containsKey("target.userName") && paramCfgMap.get("target.userName")!=null){
            userName=paramCfgMap.get("target.userName").toString();
            password=paramCfgMap.get("target.password").toString();
        }
        insertColumns=paramCfgMap.get("target.insertColumns").toString().split(",");
        executeSql=paramCfgMap.get("target.insertSql").toString();
        util=new CassandraPressitUtil(clusterName,userName,password,executeSql);
    }


    @Override
    public void writerRecord(Map<String, Object> map) {
        util.addBatchRecord(map,insertColumns);
    }

    @Override
    public void flush() throws Exception {
        util.flushRecords();
    }


}
