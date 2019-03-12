package com.robin.etl.util.fs;

import com.google.gson.stream.JsonWriter;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.xerial.snappy.SnappyOutputStream;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
public class FileFormatUtils {
    public static JsonWriter getJsonWriter(OutputStream baseStream, String compressType) throws IOException{
        return new JsonWriter(new BufferedWriter(new OutputStreamWriter(getCompressStream(baseStream,compressType))));
    }

    public static DataFileWriter<GenericRecord> getDataAvroWriter(OutputStream baseStream,Schema schema,String compressType) throws IOException{
        DatumWriter<GenericRecord> dwriter=new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> fileWriter=new DataFileWriter<GenericRecord>(dwriter);
        fileWriter.create(schema,getCompressStream(baseStream,compressType));
        return fileWriter;
    }
    public static ParquetWriter getParquetWriter(String path,Schema schema,Configuration conf,String compressType) throws IOException{
        return AvroParquetWriter.<GenericData.Record>builder(new Path(path)).withSchema(schema).withCompressionCodec(getCodec(compressType)).withConf(conf).build();
    }
    public static OutputStream getCompressStream(OutputStream baseStream,String compressType) throws IOException {
        OutputStream outputStream=null;
        if(compressType.equalsIgnoreCase("gz")){
            outputStream=new GzipCompressorOutputStream(outputStream);
        }else if(compressType.equalsIgnoreCase("snappy")){
            outputStream=new SnappyOutputStream(outputStream);
        }else if(compressType.equalsIgnoreCase("lz4")){
            LZ4Factory factory = LZ4Factory.fastestInstance();
            LZ4Compressor compressor= factory.fastCompressor();
            outputStream=new LZ4BlockOutputStream(outputStream,8192,compressor);
        }
        return outputStream;
    }
    public static CompressionCodecName getCodec(String compressType){
        CompressionCodecName codecName=CompressionCodecName.UNCOMPRESSED;
        if(compressType!=null && !compressType.isEmpty()) {
            if (compressType.equalsIgnoreCase("gz")) {
                codecName = CompressionCodecName.GZIP;
            } else if (compressType.equalsIgnoreCase("lzo")) {
                codecName = CompressionCodecName.LZO;
            } else if (compressType.equalsIgnoreCase("snappy")) {
                codecName = CompressionCodecName.SNAPPY;
            }
        }
        return codecName;
    }
    public static List<String> getFileSuffixAndCompressType(String path){
        int fileNameStartPos=path.lastIndexOf("/");
        List<String> retList=new ArrayList<>();
        if(fileNameStartPos==-1)
            fileNameStartPos=0;
        String fileName=path.substring(fileNameStartPos+1,path.length());
        String[] arr=fileName.split("\\.");
        for(int j=arr.length-1;j>0;j--){
            retList.add(arr[j]);
        }
        return retList;
    }
    public static String getCompressType(List<String> list){
        List<String> compressList= Arrays.asList(new String[]{"gz","zip","snappy","lzo","lz4","snappy","bz2"});
        String compressType=null;
        if(list!=null && list.size()>0){
            if(compressList.contains(list.get(0).toLowerCase())){
                compressType=list.get(0).toLowerCase();
            }
        }
        return compressType;
    }

}
