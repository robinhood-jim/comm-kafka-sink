package com.robin.etl.dataaccessor;

import com.robin.etl.util.fs.HdfsUtils;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;

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
public class HdfsDataAccessor extends AbstractFileSystemDataAccessor {

    private Configuration configuration;
    private HdfsUtils utils;

    public HdfsDataAccessor(Schema schema,Map<String,Object> paramCfgMap) {
        super(schema,paramCfgMap);
        this.configuration= HdfsUtils.getConf(paramCfgMap);
        utils=new HdfsUtils(configuration);
    }



    @Override
    protected OutputStream getRawOutputStream() throws IOException {
        return utils.createFile(filePath);
    }

    @Override
    public void moveTmpFileToProd(String tmpFile, String targetFile) throws IOException {
        utils.mv(tmpFile,targetFile,false);
    }
}
