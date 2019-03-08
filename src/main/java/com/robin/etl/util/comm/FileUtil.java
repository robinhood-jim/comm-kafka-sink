package com.robin.etl.util.comm;

import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * <p>Project:  kafka-sink</p>
 * <p>
 * <p>Description:com.robin.etl.util.fs</p>
 * <p>
 * <p>Copyright: Copyright (c) 2019 create at 2019年03月07日</p>
 * <p>
 * <p>Company: zhcx_DEV</p>
 *
 * @author robinjim
 * @version 1.0
 */
public class FileUtil {
    public static void checkAndCreatePath(String path,boolean includeThis) throws IOException {
        File fpath=new File(path);
        List<String> arr= Arrays.asList(fpath.toString().split("/",-1));
        int endPos=includeThis?arr.size():arr.size()-1;
        for(int i=1;i<endPos;i++){
            String tmppath= StringUtils.join(arr.subList(1,i+1),"/");
            File tmpFile=new File("/"+tmppath);
            if(!new File("/"+tmppath).isDirectory()){
                tmpFile.mkdir();
            }
        }
    }
}
