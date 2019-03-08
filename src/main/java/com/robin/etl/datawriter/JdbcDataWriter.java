package com.robin.etl.datawriter;

import com.robin.core.base.dao.SimpleJdbcDao;
import com.robin.core.base.datameta.BaseDataBaseMeta;
import com.robin.core.base.datameta.DataBaseMetaFactory;
import com.robin.core.base.datameta.DataBaseParam;
import com.robin.core.base.util.Const;
import org.apache.avro.Schema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
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
public class JdbcDataWriter extends AbstractDBLikeDataWriter {
    private BaseDataBaseMeta meta;
    private String insertSql;
    private String[] insertColumns;
    public JdbcDataWriter(Schema schema, Map<String, Object> paramCfgMap) {
        super(schema, paramCfgMap);
        if(paramCfgMap.containsKey("target.dbType")){
            if(!paramCfgMap.containsKey("target.jdbcUrl")){
                Integer port=0;
                if(paramCfgMap.containsKey("target.dbPort")){
                    port=Integer.valueOf(paramCfgMap.get("target.dbPort").toString());
                }
                meta= DataBaseMetaFactory.getDataBaseMetaByType(paramCfgMap.get("target.dbType").toString(),new DataBaseParam(paramCfgMap.get("target.dbHost").toString()
                ,port,paramCfgMap.get("target.dbSchema").toString(),paramCfgMap.get("target.dbuser").toString(),paramCfgMap.get("target.dbPassword").toString()));
            }else{
                meta= DataBaseMetaFactory.getDataBaseMetaByType(paramCfgMap.get("target.dbType").toString(),new DataBaseParam(paramCfgMap.get("target.jdbcUrl").toString()
                        ,paramCfgMap.get("target.dbUser").toString(),paramCfgMap.get("target.dbPassword").toString()));
            }
        }
        insertSql=paramCfgMap.get("target.insertSql").toString();
        insertColumns=paramCfgMap.get("target.insertColumns").toString().split(",");
    }

    @Override
    public void flush() throws Exception {
        try {
            inUsingTag = true;
            if (!records.isEmpty()) {
                executeBatchInsert();
                records.clear();
            }
        }catch (Exception ex){

        }finally {
            inUsingTag=false;
        }
    }
    protected void executeBatchInsert() throws SQLException{
        Connection connection=null;
        PreparedStatement statement=null;
        try{
            connection=SimpleJdbcDao.getConnection(meta, meta.getParam());
            connection.setAutoCommit(false);
            statement=connection.prepareStatement(insertSql);
            for(Map<String,Object> record:records){
                if(stopFlag){
                    break;
                }
                for(int i=0;i<insertColumns.length;i++) {
                    setStatements(statement,insertColumns[i],i+1,record.get(insertColumns[i]));
                }
                statement.addBatch();
            }
            if(!stopFlag){
                statement.executeBatch();
                connection.commit();
            }else{
                connection.rollback();
            }
        }catch (Exception ex){
            connection.rollback();

        }finally {
            if(statement!=null){
                statement.close();
            }
            if(connection!=null){
                connection.close();
            }
        }

    }
    protected void setStatements(PreparedStatement statement,String columnName,int pos,Object value) throws SQLException {
        if(filedTypeMap.get(columnName).equals(Const.META_TYPE_BIGINT)){
            statement.setLong(pos,(Long)value);
        }else if(filedTypeMap.get(columnName).equals(Const.META_TYPE_DOUBLE)){
            statement.setDouble(pos,(Double) value);
        }else if(filedTypeMap.get(columnName).equals(Const.META_TYPE_INTEGER)){
            statement.setInt(pos,(Integer)value);
        }else if(filedTypeMap.get(columnName).equals(Const.META_TYPE_TIMESTAMP)){
            statement.setTimestamp(pos,(Timestamp)value);
        }else if(filedTypeMap.get(columnName).equals(Const.META_TYPE_STRING)){
            statement.setString(pos,value.toString());
        }
    }

}
