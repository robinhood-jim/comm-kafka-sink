package com.robin.etl.dataaccessor;

import com.robin.etl.datawriter.CassandraDataWriter;
import com.robin.etl.util.datasink.CassandraPressitUtil;
import org.apache.avro.Schema;

import java.util.Map;

/**
 * <p>Project:  comm-kafka-sink</p>
 * <p>
 * <p>Description:</p>
 * <p>
 * <p>Copyright: Copyright (c) 2019 create at 2019年03月06日</p>
 * <p>
 * <p>Company: </p>
 *
 * @author robinjim
 * @version 1.0
 */
public class CassandraDataAccessor extends AbstractDBLikeDataAccessor {
    CassandraPressitUtil util;
    private String clusterName;
    private String userName;
    private String password;
    private String executeSql;
    private String[] insertColumns;
    public CassandraDataAccessor(Schema schema, Map<String,Object> paramCfgMap) {
        super(schema,paramCfgMap);
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
    public void writeRecord(Map<String, Object> map) throws Exception {
        util.addBatchRecord(map,insertColumns);
    }


    @Override
    protected void initDataWriter() {
        writer=new CassandraDataWriter(schema,paramCfgMap);
    }

    @Override
    protected void initDbWriter() {

    }

    @Override
    protected boolean needToFlush() {
        return false;
    }



}
