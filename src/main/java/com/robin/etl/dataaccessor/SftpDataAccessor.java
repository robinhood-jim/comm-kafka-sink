package com.robin.etl.dataaccessor;

import com.robin.core.fileaccess.util.ApacheVfsResourceAccessUtil;
import org.apache.avro.Schema;

import java.io.IOException;
import java.io.OutputStream;
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
public class SftpDataAccessor extends AbstractFileSystemDataAccessor {


    public SftpDataAccessor(Schema schema, Map<String, Object> paramCfgMap) {
        super(schema, paramCfgMap);
    }

    @Override
    protected OutputStream getRawOutputStream() throws IOException {
        return null;
    }

    @Override
    public void moveTmpFileToProd(String tmpFile, String targetFile) throws IOException {

    }



    @Override
    public void writeRecord(Map<String, Object> map) {

    }
}
