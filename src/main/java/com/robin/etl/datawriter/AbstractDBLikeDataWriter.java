package com.robin.etl.datawriter;

import com.robin.core.base.util.Const;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
public abstract class AbstractDBLikeDataWriter extends AbstractDataWriter {
    protected List<Map<String,Object>> records;
    protected Map<String,String> filedTypeMap;
    protected boolean stopFlag=false;
    protected boolean inUsingTag=false;

    public AbstractDBLikeDataWriter(Schema schema, Map<String,Object> paramCfgMap){
        this.schema=schema;
        this.paramCfgMap=paramCfgMap;
        records=new ArrayList<>();
        filedTypeMap=new HashMap<>();
        schema.getFields().forEach(field -> {
            filedTypeMap.put(field.name(),convertToCollectionMetaType(field));
        });
    }

    @Override
    public void writerRecord(Map<String, Object> map) throws IOException {
        records.add(map);
    }
    protected String convertToCollectionMetaType(Schema.Field field){
        Schema.Type seltype;
        String retType=null;
        if(field.schema().getType()==Schema.Type.UNION && field.schema().getTypes()!=null && !field.schema().getTypes().isEmpty()){
            seltype=field.schema().getTypes().get(0).getType();
        }else{
            seltype=field.schema().getType();
        }
        if(seltype== Schema.Type.INT){
            retType= Const.META_TYPE_INTEGER;
        }else if(seltype==Schema.Type.LONG){
            if(field.schema().getLogicalType()!=null){
                if(field.schema().getLogicalType()== LogicalTypes.timestampMillis()){
                    retType=Const.META_TYPE_TIMESTAMP;
                }
            }
            else{
                retType=Const.META_TYPE_BIGINT;
            }
        }else if(seltype==Schema.Type.DOUBLE){
            retType=Const.META_TYPE_DOUBLE;
        }else if(seltype==Schema.Type.FLOAT){
            retType=Const.META_TYPE_DOUBLE;
        }else if(seltype==Schema.Type.STRING){
            retType=Const.META_TYPE_STRING;
        }
        return retType;
    }

    @Override
    public void writerRecords(List<Map<String, Object>> list) throws IOException {
        records.addAll(list);
    }
    @Override
    public void close() throws Exception {
        stopFlag=true;
    }
}
