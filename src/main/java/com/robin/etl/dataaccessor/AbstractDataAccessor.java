
package com.robin.etl.dataaccessor;

import com.robin.etl.datawriter.AbstractDataWriter;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
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
public abstract class AbstractDataAccessor {
    protected Schema schema;
    protected Logger logger= LoggerFactory.getLogger(getClass());
    protected Map<String,Object> paramCfgMap;
    public AbstractDataAccessor(Schema schema,Map<String,Object> paramCfgMap){

        this.schema=schema;
        this.paramCfgMap=paramCfgMap;
    }

    //public abstract void createNewFile(String filePath) throws Exception;
    public abstract void writeRecord(Map<String,Object> map) throws Exception;
    public abstract void writeRecords(List<Map<String,Object>> list) throws Exception;
    public abstract void flush() throws Exception;

    protected abstract void initDataWriter() throws Exception;

    public Map<String, Object> getParamCfgMap() {
        return paramCfgMap;
    }

}
