package com.robin.etl.writer;

import com.robin.etl.dataaccessor.*;
import com.robin.etl.util.fs.HdfsUtils;
import org.apache.avro.Schema;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

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
public class TopicPartitionWriter {

    private Map<String, Long> offsetsMap;
    private TopicPartition topicPartition;
    private Queue<Pair<Long, Map<String, Object>>> queue = new LinkedList<>();
    private Map<String, AbstractDataAccessor> dataAccessorMap = new HashMap<>();
    private String outputTmpPath;
    private String offsetPath;
    private String outputPath;
    private String[] partitionKeys;
    private String compressType;
    private String outputFormat;
    private HdfsUtils utils;
    private int flushTimespan = 3600;
    private long lastTs;
    private boolean stopTag = false;
    private Map<String, List<Map<String, Object>>> vMap = new HashMap<>();
    private Schema schema;
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private Map<String, Pair<String, String>> fileMap = new HashMap<>();
    private Map<String, Long> partitionRowsMap = new HashMap<>();
    private int maxFileRows = 10000;
    private Logger logger = LoggerFactory.getLogger(getClass());
    private Map<String, Long> timeRangeTsMap = new HashMap<>();
    private Map<String, Object> paramCfgMap;
    private String sinkType;


    public TopicPartitionWriter(Schema schema, TopicPartition topicPartition,Map<String,Object> paramMap,Configuration conf) {
        this.paramCfgMap=paramMap;
        this.topicPartition = topicPartition;
        this.outputTmpPath = paramMap.get("outputTmpPath").toString();
        this.outputPath = paramMap.get("outputPath").toString();;
        this.offsetPath = paramMap.get("offsetPath").toString();
        this.partitionKeys = (String[])paramMap.get("partitionKeys");
        this.compressType = paramMap.get("compressType").toString();
        this.outputFormat = paramMap.get("outputFormat").toString();
        utils = new HdfsUtils(conf);
        offsetsMap = new HashMap<>();
        lastTs = System.currentTimeMillis();
        this.schema = schema;
        this.sinkType = paramMap.get("sinkType").toString();
    }

    public void writeRecord(Map<String, Object> map, Long offset) {
        queue.add(new Pair<>(offset, map));
    }

    private Pair<String, String> generateTempRandomFile(String partitionKey) {
        Long ts = System.currentTimeMillis();
        StringBuilder pathbuilder = new StringBuilder(partitionKeys[0]).append("=")
                .append(partitionKey.substring(0, 4)).append("/").append(partitionKeys[1]).append("=")
                .append(Integer.valueOf(partitionKey.substring(4, 6))).append("/").append(partitionKeys[2]).append("=")
                .append(Integer.valueOf(partitionKey.substring(6, 8))).append("/");
        StringBuilder fileNameBuilder = new StringBuilder("worker_").append(topicPartition.partition()).append("_").append(ts).append(".").append(outputFormat);
        if (compressType != null) {
            fileNameBuilder.append(".").append(compressType);
        }
        return new Pair<>(pathbuilder.toString(), fileNameBuilder.toString());
    }

    public void commitOffset(String partitionPath, Long offset) throws IOException {
        logger.info("--commit offset -- {} {}", topicPartition.partition(), offset);
        utils.mkdir(partitionPath + topicPartition.topic() + "/part-" + topicPartition.partition() + '-' + offset);
    }

    public void checkWrite() throws Exception {
        if (!stopTag) {
            vMap.clear();
            while (!queue.isEmpty()) {
                Pair<Long, Map<String, Object>> pair = queue.poll();
                putToRegion(pair);
            }
            if (!vMap.isEmpty()) {
                Iterator<Map.Entry<String, List<Map<String, Object>>>> iter = vMap.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry<String, List<Map<String, Object>>> entry = iter.next();
                    checkFile(entry.getKey());
                    if (!dataAccessorMap.containsKey(entry.getKey())) {
                        checkFile(entry.getKey());
                    }
                    flush(entry.getKey());
                }
                lastTs = System.currentTimeMillis();
            }
            if (needFlush() || checkRecordSize()) {
                flushAndClose();
            }
        }
    }

    private boolean needFlush() {
        return System.currentTimeMillis() - lastTs >= flushTimespan * 1000;
    }

    private boolean checkRecordSize() {
        Iterator<Map.Entry<String, Long>> iter = partitionRowsMap.entrySet().iterator();
        boolean needFlush = false;
        while (iter.hasNext()) {
            Map.Entry<String, Long> entry = iter.next();

            if (entry.getValue() > maxFileRows) {
                needFlush = true;
                break;
            }
        }
        return needFlush;
    }

    private void addRows(String partitionKey, int size) {
        long totalrows;
        if (!partitionRowsMap.containsKey(partitionKey)) {
            totalrows = Long.valueOf(size);
        } else {
            totalrows = partitionRowsMap.get(partitionKey) + size;
        }
        //logger.error("--flush to range {} with rows {}",partitionKeys,totalrows);
        partitionRowsMap.put(partitionKey, totalrows);
        timeRangeTsMap.put(partitionKey, System.currentTimeMillis());
    }

    private void checkFile(String partitionKey) {
        if (dataAccessorMap.get(partitionKey) == null) {
            Pair<String, String> pair = generateTempRandomFile(partitionKey);
            if(logger.isDebugEnabled())
                logger.debug("--write to {}", outputTmpPath + pair.getKey() + pair.getValue());
            paramCfgMap.put("outputFile", outputTmpPath + pair.getKey() + pair.getValue());
            AbstractDataAccessor accessor = getDataAccessor();
            dataAccessorMap.put(partitionKey, accessor);
            fileMap.put(partitionKey, pair);
        }
    }

    private void flushAndClose() throws Exception {
        if(logger.isDebugEnabled())
            logger.debug("--begin to flush--");
        if (!dataAccessorMap.isEmpty()) {
            Iterator<Map.Entry<String, AbstractDataAccessor>> iter = dataAccessorMap.entrySet().iterator();
            Long offset = 0L;

            while (iter.hasNext()) {
                Map.Entry<String, AbstractDataAccessor> entry = iter.next();
                Pair<String, String> pair = generateTempRandomFile(entry.getKey());
                if(entry.getValue() instanceof AbstractDBLikeDataAccessor) {
                    entry.getValue().flush();
                }
                else if(entry.getValue() instanceof AbstractFileSystemDataAccessor){
                    ((AbstractFileSystemDataAccessor)entry.getValue()).flushAndClose();
                    //File type Data Accessor need to move tmp file to production
                    ((AbstractFileSystemDataAccessor) entry.getValue()).moveTmpFileToProd(outputTmpPath + fileMap.get(entry.getKey()).getKey() + fileMap.get(entry.getKey()).getValue(),
                            outputPath + fileMap.get(entry.getKey()).getKey() + fileMap.get(entry.getKey()).getValue());
                }

                if (offset == 0L) {
                    offset = offsetsMap.get(entry.getKey());
                } else if (offset < offsetsMap.get(entry.getKey())) {
                    offset = offsetsMap.get(entry.getKey());
                }
            }
            dataAccessorMap.clear();
            //mark offset checkpoint
            commitOffset(offsetPath, offset);
            offsetsMap.clear();
            partitionRowsMap.clear();
            lastTs = System.currentTimeMillis();
        }
    }

    private void flush(String timeRange) throws Exception{
        int size = vMap.get(timeRange).size();
        dataAccessorMap.get(timeRange).writeRecords(vMap.get(timeRange));
        addRows(timeRange, size);
    }


    public AbstractDataAccessor getDataAccessor() {
        AbstractDataAccessor dataAccessor = null;
        if (sinkType.equalsIgnoreCase("hdfs")) {
            dataAccessor = new HdfsDataAccessor(schema, paramCfgMap);
        } else if (sinkType.equalsIgnoreCase("cassandra")) {
            dataAccessor = new CassandraDataAccessor(schema, paramCfgMap);
        } else if (sinkType.equalsIgnoreCase("jdbc")) {
            dataAccessor = new JdbcDataAccessor(schema, paramCfgMap);
        }else if(sinkType.equalsIgnoreCase("ftp")){

        }else if(sinkType.equalsIgnoreCase("local")){
            dataAccessor=new LocalFileDataAccessor(schema,paramCfgMap);
        }
        if(dataAccessor instanceof AbstractFileSystemDataAccessor){
            ((AbstractFileSystemDataAccessor)dataAccessor).beginWrite();
        }
        return dataAccessor;
    }

    public void putToRegion(Pair<Long, Map<String, Object>> pair) {
        try {
            Map<String, Object> map = pair.getValue();
            String partVal = map.get("$PARTITION").toString();
            map.remove("$PARTITION");
            if (!vMap.containsKey(partVal)) {
                List<Map<String, Object>> list = new ArrayList<>();
                list.add(map);
                vMap.put(partVal, list);
            } else
                vMap.get(partVal).add(map);
            offsetsMap.put(partVal, pair.getKey());
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }


}
