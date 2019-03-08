package com.robin.etl.dataaccessor;

import com.robin.etl.util.comm.FileUtil;
import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileOutputStream;
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
public class LocalFileDataAccessor extends AbstractFileSystemDataAccessor {
    public LocalFileDataAccessor(Schema schema,Map<String,Object> paramCfg) {
        super(schema,paramCfg);
        try {
            FileUtil.checkAndCreatePath(filePath,false);
        }catch (Exception ex){
            logger.error("",ex);
        }
    }

    @Override
    protected OutputStream getRawOutputStream() throws IOException {
        return new FileOutputStream(new File(filePath));
    }


    @Override
    public void writeRecord(Map<String, Object> map) throws IOException {
        writer.writeRecord(map);
    }

    @Override
    public void moveTmpFileToProd(String tmpFile, String targetFile) throws IOException {
        FileUtils.moveFile(new File(tmpFile),new File(targetFile));
    }
}
