package com.robin.etl.datawriter;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
public abstract class AbstractDataWriter {
    protected Schema schema;
    protected Logger logger= LoggerFactory.getLogger(getClass());
    protected Map<String,Object> paramCfgMap;

    public abstract void writerRecord(Map<String,Object> map) throws IOException;
    public abstract void writerRecords(List<Map<String,Object>> map) throws IOException;
    public abstract void flush() throws Exception;
    public abstract void close() throws Exception;

}
