package com.robin.etl.kafka.sink;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.robin.core.base.util.Const;

import com.robin.core.fileaccess.util.AvroUtils;
import com.robin.etl.util.fs.HdfsUtils;
import com.robin.etl.util.json.GsonUtil;

import com.robin.etl.writer.TopicPartitionWriter;
import org.apache.avro.Schema;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import com.robin.core.fileaccess.meta.DataCollectionMeta;


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
public class CommKafkaPartitionSink implements Callable<Integer> {
    private final KafkaConsumer<Integer, String> consumer;
    private final String topic;
    private final String partitionColumn;
    private final String[] selectColumns;
    private final String[] insertColumns;
    private final String[] insertColumnTypes;
    private Logger log= LoggerFactory.getLogger(getClass());
    private boolean stopTag=false;
    private Gson gson = GsonUtil.getGson();
    private int maxFileRows =1000;
    private SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private SimpleDateFormat transFormat=new SimpleDateFormat("yyyyMMdd");
    private int pollSeconds=10;
    private boolean failOnException=true;
    private String offsetPath="/tmp/"+System.getProperty("user.name")+"/offsets-path/";
    private String outputTmpPath="/tmp/"+System.getProperty("user.name")+"/kafkadump/";
    private Map<Long,Integer> retryTimesMap=new HashMap<>();
    private int maxRetryTimes=3;
    private int partitionNums=-1;
    private Map<TopicPartition, TopicPartitionWriter> topicPartitionWriterMap;
    private Set<TopicPartitionWriter> writerSets=new HashSet<>();
    private String outputPath;
    private String defaultFs;
    private String outputFormat;
    private String compressType;
    private String[] partitionKeys;
    private Configuration conf;
    private Schema schema;
    private List<TopicPartition> partitions=new ArrayList<>();
    private Map<Integer,TopicPartition> pMap=new HashMap<>();
    private Map<Integer,Long> posMap=new HashMap<>();
    private boolean withAssign=false;
    private String sinkType="local";
    private String sourceFormat="json";
    private String sourceSeparator=",";
    ListeningExecutorService scanjobPool= MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));

    public CommKafkaPartitionSink(ResourceBundle bundle){
        Properties props = new Properties();
        String brokerUrl=bundle.getString("brokerUrl");
        String groupId=bundle.getString("groupId");
        log.info("---"+brokerUrl+"-----"+groupId);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        this.topic = bundle.getString("source_topic");
        this.partitionColumn=bundle.getString("trans.partitonColumn");
        this.selectColumns=bundle.getString("trans.selectColumns").split(",");
        this.insertColumns=bundle.getString("trans.insertColumns").split(",");
        this.insertColumnTypes=bundle.getString("trans.insertColumnTypes").split(",");
        DataCollectionMeta meta=new DataCollectionMeta();
        for(int i=0;i<insertColumns.length;i++){
            meta.addColumnMeta(insertColumns[i],insertColumnTypes[i],null);
        }
        schema = AvroUtils.getSchemaFromMeta(meta);
        Preconditions.checkArgument(selectColumns.length==insertColumns.length);
        Preconditions.checkArgument(selectColumns.length==insertColumnTypes.length);
        this.partitionKeys = (bundle.containsKey("trans.partitionKeys"))?bundle.getString("trans.partitionKeys").split(","):new String[]{"time_year","time_month","time_day"};
        this.outputPath = bundle.getString("trans.flushBasePath");
        this.outputFormat = bundle.getString("trans.outputFileFormat");
        this.compressType = bundle.getString("trans.compressType");
        this.defaultFs = bundle.getString("trans.defaultFs");
        if(bundle.containsKey("trans.outputSinkType")){
            this.sinkType=bundle.getString("trans.outputSinkType");
        }
        if(bundle.containsKey("source.format")){
            sourceFormat=bundle.getString("source.format");
        }
        if(bundle.containsKey("source.separator")){
            sourceSeparator=bundle.getString("source.separator");
        }
        conf=new Configuration(false);
        conf.set("fs.defaultFS",defaultFs);

        if (bundle.containsKey("trans.maxFileRows") && bundle.getString("trans.maxFileRows") != null) {
            maxFileRows = Integer.valueOf(bundle.getString("trans.maxFileRows"));
        }
        if(bundle.containsKey("trans.dateFormat") && !bundle.getString("trans.dateFormat").isEmpty()){
            format=new SimpleDateFormat(bundle.getString("trans.dateFormat"));
        }
        if(bundle.containsKey("trans.pollSeconds") && !bundle.getString("trans.pollSeconds").isEmpty()){
            pollSeconds=Integer.valueOf(bundle.getString("trans.pollSeconds"));
        }
        if(bundle.containsKey("trans.failOnException") && bundle.getString("trans.failOnException").equalsIgnoreCase("false")){
            failOnException=false;
        }
        if(bundle.containsKey("trans.offsetPath") && bundle.getString("trans.offsetPath")!=null && !bundle.getString("trans.offsetPath").isEmpty()){
            offsetPath=bundle.getString("trans.offsetPath");
        }
        if(bundle.containsKey("trans.outputTmpPath")){
            outputTmpPath=bundle.getString("trans.outputTmpPath");
        }

        consumer = new KafkaConsumer<>(props);

        try {
            //get Partition Info
            AdminClient client = AdminClient.create(props);
            DescribeTopicsResult rs = client.describeTopics(Arrays.asList(topic));
            Map<String, KafkaFuture<TopicDescription>> values = rs.values();
            KafkaFuture<TopicDescription> topicDescription = values.get(topic);
            partitionNums = topicDescription.get().partitions().size();
            topicPartitionWriterMap=new HashMap<>();
            for(TopicPartitionInfo info:topicDescription.get().partitions()){
                Map<String,Object> map=wrapParameter();
                TopicPartitionWriter writer=new TopicPartitionWriter(schema,new TopicPartition(topic,info.partition()),map,conf);
                topicPartitionWriterMap.put(new TopicPartition(topic,info.partition()),writer);
                TopicPartition tp=new TopicPartition(topic,info.partition());
                partitions.add(tp);
                pMap.put(info.partition(),tp);
                writerSets.add(writer);
            }
            //recover from last checkPoint
            restoreOffsets();
            scanjobPool.submit(new CleanOffsetJob());

        }catch (Exception ex){
            ex.printStackTrace();
        }

    }
    @Override
    public Integer call() throws Exception {
        Map<String, List<Map<String,Object>>> valueMap=new HashMap<>();
        if(consumer==null || partitionNums==-1){
            throw new RuntimeException("kafka consumer init Error!");
        }
        try {
            while (!stopTag) {
                if(!withAssign)
                    consumer.subscribe(Collections.singletonList(this.topic));
                ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(pollSeconds));
                valueMap.clear();
                boolean runningOk = true;

                for (ConsumerRecord<Integer, String> record : records) {
                    runningOk = doParseRecord(record);
                    //meet exception need to re consume
                    if (!runningOk) {
                        valueMap.clear();
                        consumer.seek(pMap.get(record.partition()), record.offset());
                        //出错重试，超过次数报错
                        if (!retryTimesMap.containsKey(record.offset())) {
                            retryTimesMap.put(record.offset(), 1);
                        } else {
                            if (retryTimesMap.get(record.offset()) + 1 > maxRetryTimes) {
                                throw new RuntimeException("process record at " + record.topic() + " " + record.partition() + ",offset " + record.offset() + " failed!content:" + record.value());
                            } else {
                                retryTimesMap.put(record.offset(), retryTimesMap.get(record.offset()) + 1);
                            }
                        }
                        break;
                    }
                }
                //check row max than maximize rows.then all partition should sumbit and create New files;

                for (TopicPartitionWriter w : writerSets) {
                    w.checkWrite();
                }

            }
        }catch (Exception ex){
            ex.printStackTrace();
        }
        if(consumer!=null){
            consumer.close();
        }
        return 0;
    }
    private boolean doParseRecord(ConsumerRecord<Integer,String> record){
        boolean encounterException=false;
        try {

            Map<String, Object> gfb = readFromSource(record.value());
            if (gfb.containsKey(partitionColumn)) {
                Map<String,Object> vmap=wrapRecord(gfb);
                String partitionValue = getPartitionValue(gfb, partitionColumn);
                //partition key
                vmap.put("$PARTITION",partitionValue);
                topicPartitionWriterMap.get(new TopicPartition(record.topic(),record.partition())).writeRecord(vmap,record.offset());
            }
        }catch (Exception ex){
            log.info("--error parse record {}",record);
            encounterException=true;
        }
        if(failOnException){
            return !encounterException;
        }else {
            return true;
        }
    }
    protected Map<String,Object> readFromSource(String value){
        Map<String,Object> retMap=new HashMap<>();
        if(sourceFormat.equalsIgnoreCase("json")){
            retMap=gson.fromJson(value, new TypeToken<Map<String, Object>>() {
            }.getType());
        }else if(sourceFormat.equalsIgnoreCase("csv")){
            String[] arr=value.split(sourceSeparator);
            if(arr.length>=schema.getFields().size()){
                for(int i=0;i<schema.getFields().size();i++){
                    retMap.put(schema.getFields().get(i).name(),arr[i]);
                }
            }
        }else if(sourceFormat.equalsIgnoreCase("avro")){

        }
        return retMap;
    }
    //recover from checkpoint path
    private void restoreOffsets(){
        try {
            Map<Integer, Long> tmpOffsetsMap = retOffsetMapAndClean();
            if(tmpOffsetsMap!=null && !tmpOffsetsMap.isEmpty()) {
                withAssign=true;
                Iterator<Map.Entry<Integer, Long>> iter = tmpOffsetsMap.entrySet().iterator();
                consumer.assign(partitions);
                while (iter.hasNext()) {
                    Map.Entry<Integer, Long> entry = iter.next();
                    log.info("reset topic {} partition{} to offset{}", topic, entry.getKey(), entry.getValue());
                    consumer.seek(pMap.get(entry.getKey()),entry.getValue());
                }
            }

        }catch (Exception ex){
            ex.printStackTrace();
        }

    }
    private Pair<Integer,Long> getOffset(String[] arr){
        try{
            Preconditions.checkArgument(NumberUtils.isDigits(arr[1]));
            Preconditions.checkArgument(NumberUtils.isDigits(arr[2]));
            return new Pair<>(Integer.valueOf(arr[1]),Long.valueOf(arr[2]));

        }catch (Exception ex){

        }
        return null;
    }

    private Map<String,Object> wrapRecord(Map<String,Object> inputMap){
        Map<String,Object> retMap=new HashMap<>();
        for(int i=0;i<selectColumns.length;i++){
            if(inputMap.containsKey(selectColumns[i]) && inputMap.get(selectColumns[i])!=null){
                retMap.put(insertColumns[i],convertWithType(inputMap.get(selectColumns[i]),insertColumnTypes[i]));
            }
        }
        return retMap;
    }
    private Object convertWithType(Object value,String columnType){
        Object retValue=null;
        if(columnType.equals(Const.META_TYPE_INTEGER)){
            if(value instanceof Integer){
                retValue=value;
            }
            else if(NumberUtils.isDigits(value.toString())){
                retValue=Integer.valueOf(value.toString());
            }
        }else if(columnType.equals(Const.META_TYPE_DOUBLE) || columnType.equals(Const.META_TYPE_NUMERIC)){
            if(value instanceof Double){
                retValue=value;
            }else if(NumberUtils.isNumber(value.toString())){
                retValue=Double.valueOf(value.toString());
            }
        }else if(columnType.equals(Const.META_TYPE_BIGINT)){
            if(value instanceof Long){
                retValue=value;
            }else if(NumberUtils.isDigits(value.toString())){
                retValue=Long.valueOf(value.toString());
            }
        }else if(columnType.equals(Const.META_TYPE_TIMESTAMP)){
            if(value instanceof Long){
                retValue=new Timestamp((Long)value);
            }else if(NumberUtils.isDigits(value.toString())){
                retValue=new Timestamp(Long.valueOf(value.toString()));
            }else{
                try{
                    Date date=format.parse(value.toString());
                    retValue=new Timestamp(date.getTime());
                }catch (ParseException ex){

                }
            }
            if(outputFormat.equalsIgnoreCase("parquet") || outputFormat.equalsIgnoreCase("avro")){
                retValue=((Timestamp)retValue).getTime();
            }
        }else if(columnType.equals(Const.META_TYPE_STRING)){
            retValue=value.toString();
        }
        return retValue;
    }
    private String getPartitionValue(Map<String,Object> map,String partitionColumn){
        Object obj=map.get(partitionColumn);
        Timestamp ts=null;
        try {
            if (obj != null && !obj.toString().isEmpty()) {
                if (NumberUtils.isNumber(obj.toString())) {
                    ts = new Timestamp(Long.valueOf(obj.toString()));
                } else {
                    ts = new Timestamp(format.parse(obj.toString()).getTime());
                }
            }
        }catch (Exception ex){

        }
        if(ts!=null){
            return transFormat.format(ts);
        }else{
            return null;
        }
    }
    public void close(){
        stopTag=true;
        if(consumer!=null){
            consumer.close();
        }
    }
    //clean partition path by time
    public class CleanOffsetJob implements Callable<Integer>{

        @Override
        public Integer call() throws Exception {
            while(!stopTag){
                retOffsetMapAndClean();
                Thread.sleep(3600000L);
            }
            return 1;
        }
    }
    protected Map<String,Object> wrapParameter(){
        Map<String,Object> map=new HashMap<>();
        map.put("partitionColumn",partitionColumn);
        map.put("fs.defaultFS",defaultFs);
        map.put("partitionKeys",partitionKeys);
        map.put("selectColumns",selectColumns);
        map.put("insertColumns",insertColumns);
        map.put("sinkType",sinkType);
        map.put("insertColumnTypes",insertColumnTypes);
        map.put("outputPath",outputPath);
        map.put("outputTmpPath",outputTmpPath);
        map.put("compressType",compressType);
        map.put("sourceFormat",sourceFormat);
        map.put("offsetPath",offsetPath);
        map.put("outputFormat",outputFormat);
        return map;
    }

    public Map<Integer, Long> retOffsetMapAndClean(){
        Map<Integer, Long> tmpOffsetsMap = new HashMap<>();
        try{
            HdfsUtils.checkAndCreatePath(conf,offsetPath + topic,true);
            List<String> filePaths = HdfsUtils.listPathName(conf, offsetPath + topic, "part-", null);

            Map<Integer,String> currentOffsetMap=new HashMap<>();
            for (String name : filePaths) {
                String[] arr = name.split("-");
                Pair<Integer, Long> pair = getOffset(arr);
                if (pair != null) {
                    if (!tmpOffsetsMap.containsKey(pair.getKey())) {
                        tmpOffsetsMap.put(pair.getKey(), pair.getValue());
                        currentOffsetMap.put(pair.getKey(),name);
                    } else {
                        if (tmpOffsetsMap.get(pair.getKey()) < pair.getValue()) {
                            tmpOffsetsMap.put(pair.getKey(), pair.getValue());
                            currentOffsetMap.put(pair.getKey(),name);
                        }
                    }
                }
            }
            Iterator<Map.Entry<Integer, Long>> iter = tmpOffsetsMap.entrySet().iterator();
            while(iter.hasNext()){
                filePaths.remove(currentOffsetMap.get(iter.next().getKey()));
            }
            for(String path:filePaths){
                log.info("---remove offset path {}",path);
                HdfsUtils.delete(conf,offsetPath + topic+"/"+path);
            }
        }catch (Exception ex){
            log.error("",ex);
        }
        return tmpOffsetsMap;
    }

}
