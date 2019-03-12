package com.robin.etl.dataaccessor;

import com.robin.etl.datawriter.AbstractDBLikeDataWriter;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.List;
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
public abstract class AbstractDBLikeDataAccessor extends AbstractDataAccessor {

    protected AbstractDBLikeDataWriter writer;
    protected long batchRows=10000L;
    protected long curRows=0L;

    public AbstractDBLikeDataAccessor(Schema schema, Map<String, Object> paramCfgMap) {
        super(schema, paramCfgMap);
    }

    @Override
    public void writeRecord(Map<String, Object> map) throws Exception {
        writer.writerRecord(map);
        curRows++;
        checkToFlush();
    }
    public void writeRecords(List<Map<String,Object>> list) throws Exception{
        writer.writerRecords(list);
        curRows+=list.size();
        checkToFlush();
    }
    protected void checkToFlush() throws Exception{
        if(needToFlush()){
            flush();
            afterProcess();
            curRows=0L;
        }
    }
    @Override
    protected void initDataWriter() throws Exception {

    }

    protected abstract void initDbWriter();

    protected boolean needToFlush(){
        return curRows>=batchRows;
    }

    @Override
    public void flush() throws Exception{
        writer.flush();
    }

    protected void afterProcess(){

    }
    protected void close(){

    }
}
