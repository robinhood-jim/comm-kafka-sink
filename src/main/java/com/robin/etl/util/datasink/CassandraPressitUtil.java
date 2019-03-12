package com.robin.etl.util.datasink;

import com.datastax.driver.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * <p>Project:  kafka-tile38-loader</p>
 * <p>
 * <p>Description:com.zhcx.cassandra</p>
 * <p>
 * <p>Copyright: Copyright (c) 2019 create at 2019年01月31日</p>
 * <p>
 * <p>Company: zhcx_DEV</p>
 *
 * @author robinjim
 * @version 1.0
 */
public class CassandraPressitUtil {
    private String clusterNames;
    private String userName;
    private String password;
    private Cluster cluster;
    private Session session;

    private PreparedStatement statement;
    private BoundStatement boundStatement;
    private String sql;
    private BatchStatement batchStatement;
    private Logger logger= LoggerFactory.getLogger(getClass());
    public CassandraPressitUtil(String clusterNames, String userName, String password, String sql){
        this.clusterNames=clusterNames;
        this.userName=userName;
        this.password=password;
        getSession();
        this.sql=sql;
        statement=session.prepare(sql);
        boundStatement=new BoundStatement(statement);
        batchStatement=new BatchStatement();

    }

    private void getSession(){
        Cluster.Builder builder = Cluster.builder().withoutJMXReporting().addContactPoints(clusterNames.split(","));
        if(userName!=null && password!=null && !userName.isEmpty() && !password.isEmpty()) {
            builder = builder.withAuthProvider(new PlainTextAuthProvider(userName, password));
        }
        cluster = builder.build();
        session= cluster.connect();
    }

    public void addRecordWithBatchSize(Object[] objects,int batchSize){
        if(batchStatement.size()>=batchSize){
            batchInsert();
            batchStatement.clear();
        }
        batchStatement.add(boundStatement.bind(objects));
    }
    public void addBatchRecord(Object[] objects){
        batchStatement.add(boundStatement.bind(objects));
    }
    public void addBatchRecord(Map<String, Object> valueMap,String[] insertColumns){
        Object[] objs=new Object[insertColumns.length];
        for(int i=0;i<insertColumns.length;i++){
            String insertColumn=insertColumns[i];
            if(valueMap.containsKey(insertColumn)){
                objs[i]=valueMap.get(insertColumn);
            }else{
                objs[i]=null;
            }
        }
        batchStatement.add(boundStatement.bind(objs));
    }
    public void flushRecords(){
        batchInsert();
        batchStatement.clear();
    }
    public void flushRemains(){
        if(batchStatement.size()>0){
            batchInsert();
        }
    }
    public void addRecord(Object[] objects){
        session.execute(boundStatement.bind(objects));
    }
    public void batchInsert(){
        session.execute(batchStatement);
    }
    public void close(){
        try {
            session.close();
        }catch (Exception ex){
            logger.error("{}",ex);
        }

    }
}
