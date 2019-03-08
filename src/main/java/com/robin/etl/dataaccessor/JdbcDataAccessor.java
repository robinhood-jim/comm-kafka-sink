package com.robin.etl.dataaccessor;

import com.robin.etl.datawriter.JdbcDataWriter;
import org.apache.avro.Schema;

import java.io.IOException;
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
public class JdbcDataAccessor extends AbstractDBLikeDataAccessor {


    public JdbcDataAccessor(Schema schema, Map<String, Object> paramCfgMap) {
        super(schema, paramCfgMap);
    }

    @Override
    protected void initDbWriter() {
        writer=new JdbcDataWriter(schema,paramCfgMap);
    }

    @Override
    protected boolean needToFlush() {
        return false;
    }



}
