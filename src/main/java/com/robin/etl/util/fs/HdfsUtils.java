package com.robin.etl.util.fs;

import com.robin.core.base.util.Const;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
public class HdfsUtils {
    private Configuration configuration;

    public HdfsUtils() {
        configuration = new Configuration();
    }

    public HdfsUtils(String defaultFs) {
        configuration = new Configuration(false);
        configuration.set("fs.defaultFS", defaultFs);
    }

    public HdfsUtils(Configuration configuration) {
        this.configuration = configuration;
    }

    public boolean exists(String path) throws IOException {
        return getFs().exists(new Path(path));
    }

    public FSDataOutputStream createFile(String path) throws IOException {
        return getFs().create(new Path(path));
    }

    public FileSystem getFs() throws IOException {
        return FileSystem.get(configuration);
    }
    public static FileSystem getFs(final Configuration configuration) throws IOException {
        return FileSystem.get(configuration);
    }
    public static boolean delete(final Configuration configuration,String path) throws IOException{
        return getFs(configuration).delete(new Path(path),true);
    }
    public static Configuration getConf(Map<String,Object> paramMap){
        Configuration conf=new Configuration(false);
        if(paramMap.containsKey(Const.HDFS_NAME_HADOOP2)){
            conf.set(Const.HDFS_NAME_HADOOP2,paramMap.get(Const.HDFS_NAME_HADOOP2).toString());
        }else if(paramMap.containsKey(Const.HDFS_NAME_HADOOP1)){
            conf.set(Const.HDFS_NAME_HADOOP1,paramMap.get(Const.HDFS_NAME_HADOOP1).toString());
        }
        return conf;
    }

    public Configuration getConf() {
        return configuration;
    }

    public static List<String> listPath(Configuration configuration, String hdfsUrl, String nameprefix, String namesuffix) throws IOException {
        List<String> hdfsUrlList = new ArrayList<String>();
        try {
            FileSystem fs = FileSystem.get(configuration);
            Path path = new Path(hdfsUrl);
            FileStatus[] status = fs.listStatus(path);
            Path[] listPaths = FileUtil.stat2Paths(status);
            for (int i = 0; i < listPaths.length; i++) {
                if(match(listPaths[i],nameprefix,namesuffix)){
                    hdfsUrlList.add(listPaths[i].toString());
                }
            }
        } catch (IOException e) {
            throw e;
        }
        return hdfsUrlList;
    }
    public static List<String> listDirectory(Configuration configuration, String hdfsUrl, String nameprefix, String namesuffix) throws IOException {
        List<String> hdfsUrlList = new ArrayList<String>();
        try {
            FileSystem fs = FileSystem.get(configuration);
            Path path = new Path(hdfsUrl);
            FileStatus[] status = fs.listStatus(path);
            Path[] listPaths = FileUtil.stat2Paths(status);
            for (int i = 0; i < listPaths.length; i++) {
                if(status[i].isDirectory()){
                    hdfsUrlList.add(listPaths[i].toString());
                }
            }
        } catch (IOException e) {
            throw e;
        }
        return hdfsUrlList;
    }
    public List<String> listPath(String hdfsUrl, String nameprefix, String namesuffix) throws IOException {
        return listPath(configuration,hdfsUrl,nameprefix,namesuffix);
    }
    public static List<String> listPathName(Configuration configuration,String hdfsUrl, String nameprefix, String namesuffix) throws IOException {
        List<String> hdfsUrlList = new ArrayList<String>();
        try {
            FileSystem fs = FileSystem.get(configuration);
            Path path = new Path(hdfsUrl);
            FileStatus[] status = fs.listStatus(path);
            Path[] listPaths = FileUtil.stat2Paths(status);
            for (int i = 0; i < listPaths.length; i++) {
                if(match(listPaths[i],nameprefix,namesuffix)){
                    hdfsUrlList.add(listPaths[i].getName());
                }
            }
        } catch (IOException e) {
            throw e;
        }
        return hdfsUrlList;
    }
    public List<String> listPathName(String hdfsUrl, String nameprefix, String namesuffix) throws IOException {
        return listPathName(configuration,hdfsUrl,nameprefix,namesuffix);
    }
    public boolean mkdir(String path) throws IOException{
        return getFs().mkdirs(new Path(path));
    }
    public static boolean mkdir(Configuration configuration,String path) throws IOException{
        return FileSystem.get(configuration).mkdirs(new Path(path));
    }
    public boolean mv(String source,String target,boolean containLastName) throws IOException{
        checkAndCreatePath(target,containLastName);
        return getFs().rename(new Path(source),new Path(target));
    }
    public boolean checkAndCreatePath(String path,boolean includeThis) throws IOException{
        File fpath=new File(path);

        List<String> arr= Arrays.asList(path.split("/",-1));
        int endPos=includeThis?arr.size():arr.size()-1;
        for(int i=1;i<endPos;i++){
            String tmppath= StringUtils.join(arr.subList(1,i+1),"/");
            if(!exists("/"+tmppath)){
                mkdir("/"+tmppath);
            }
        }
        return true;
    }
    public static boolean mv(Configuration configuration, String source,String target) throws IOException{
        return FileSystem.get(configuration).rename(new Path(source),new Path(target));
    }

    private static boolean match(Path filePath, String nameprefix, String namesuffix) {
        boolean fit = false;
        if (!filePath.getName().equals("_SUCCESS")) {
            if (nameprefix != null && !nameprefix.isEmpty()) {
                if (filePath.getName().startsWith(nameprefix)) {
                    fit = true;
                } else if (namesuffix != null && !namesuffix.isEmpty()) {
                    if (filePath.getName().endsWith(nameprefix)) {
                        fit = true;
                    }
                } else {
                    fit = true;
                }
            } else {

            }
        }
        return fit;
    }
    public static void main(String[] args){
        HdfsUtils utils=new HdfsUtils("hdfs://master1.cloud.123cx.com:8020");
        try {
            utils.checkAndCreatePath("/tmp/luoming/testout1/time_year=2019/time_month=1/time_day=1",true);
        }catch (Exception ex){
            ex.printStackTrace();
        }

    }

}
