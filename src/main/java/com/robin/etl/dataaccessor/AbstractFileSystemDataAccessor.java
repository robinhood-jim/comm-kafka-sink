package com.robin.etl.dataaccessor;

import com.robin.core.base.util.Const;
import com.robin.core.fileaccess.meta.DataCollectionMeta;
import com.robin.core.fileaccess.writer.AbstractFileWriter;
import com.robin.core.fileaccess.writer.TextFileWriterFactory;
import com.robin.etl.util.fs.FileFormatUtils;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.anarres.lzo.LzoAlgorithm;
import org.anarres.lzo.LzoCompressor;
import org.anarres.lzo.LzoLibrary;
import org.anarres.lzo.LzoOutputStream;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.LZMAOutputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

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
public abstract class AbstractFileSystemDataAccessor extends AbstractDataAccessor {
    protected String filePath;
    protected String compressType;
    protected String outFileFormat;
    protected AbstractFileWriter writer;
    protected DataCollectionMeta colmeta;
    protected OutputStream outputStream;

    public AbstractFileSystemDataAccessor(Schema schema, Map<String, Object> paramCfgMap) {
        super(schema, paramCfgMap);
        this.compressType = paramCfgMap.get("compressType").toString();
        if (paramCfgMap.containsKey("outputFile")) {
            this.filePath = paramCfgMap.get("outputFile").toString();
        }
        if (paramCfgMap.containsKey("outFileFormat") && paramCfgMap.get("outFileFormat") != null)
            this.outFileFormat = paramCfgMap.get("outFileFormat").toString();

        if (outFileFormat == null && filePath != null) {
            List<String> suffixList = FileFormatUtils.getFileSuffixAndCompressType(filePath);
            compressType = FileFormatUtils.getCompressType(suffixList);
            outFileFormat = compressType == null ? suffixList.get(0) : suffixList.get(1);
        }
        if (outFileFormat != null) {
            paramCfgMap.put("outFileFormat", outFileFormat);
        }
        initDataWriter();
        constructMeta();
        try {
            outputStream = getOutputStreamBySuffix(compressType, getRawOutputStream());
        } catch (Exception ex) {
            logger.error("", ex);
        }
    }

    protected void initDataWriter() {
        writer = TextFileWriterFactory.getFileWriterByType(outFileFormat, colmeta, outputStream);
    }

    public void flush() throws Exception {
        if (writer != null)
            writer.flush();
    }

    public void close() throws Exception {
        if (writer != null) {
            writer.flush();
            writer.close();
        }
    }

    public void flushAndClose() throws Exception {
        if (writer != null) {
            writer.flush();
            writer.close();
        }
    }

    @Override
    public void writeRecord(Map<String, Object> map) throws IOException {
        writer.writeRecord(map);
    }

    @Override
    public void writeRecords(List<Map<String, Object>> list) throws IOException {
        for (Map<String, Object> map : list) {
            writer.writeRecord(map);
        }
    }

    protected abstract OutputStream getRawOutputStream() throws IOException;

    protected void constructMeta() {
        colmeta = new DataCollectionMeta();
        colmeta.setResourceCfgMap(paramCfgMap);
        List<Schema.Field> fields = schema.getFields();
        fields.forEach(field -> {
            addField(field);
        });
    }

    private void addField(Schema.Field field) {
        Schema.Type seltype = null;
        if (field.schema().getType() == Schema.Type.UNION && field.schema().getTypes() != null && !field.schema().getTypes().isEmpty()) {
            seltype = field.schema().getTypes().get(0).getType();
        } else {
            seltype = field.schema().getType();
        }
        if (seltype == Schema.Type.INT) {
            colmeta.addColumnMeta(field.name(), Const.META_TYPE_INTEGER, null);
        } else if (seltype == Schema.Type.LONG) {
            if (field.schema().getLogicalType() != null) {
                if (field.schema().getLogicalType() == LogicalTypes.timestampMillis()) {
                    colmeta.addColumnMeta(field.name(), Const.META_TYPE_TIMESTAMP, null);
                }
            } else {
                colmeta.addColumnMeta(field.name(), Const.META_TYPE_BIGINT, null);
            }
        } else if (seltype == Schema.Type.DOUBLE) {
            colmeta.addColumnMeta(field.name(), Const.META_TYPE_DOUBLE, null);
        } else if (seltype == Schema.Type.FLOAT) {
            colmeta.addColumnMeta(field.name(), Const.META_TYPE_DOUBLE, null);
        } else if (seltype == Schema.Type.STRING) {
            colmeta.addColumnMeta(field.name(), Const.META_TYPE_STRING, null);
        }
    }

    protected OutputStream getOutputStreamBySuffix(String suffix, OutputStream out) throws IOException {
        OutputStream outputStream = null;
        if (Const.SUFFIX_GZIP.equalsIgnoreCase(suffix)) {
            outputStream = new GZIPOutputStream(wrapOutputStream(out));
        } else if (Const.SUFFIX_ZIP.equalsIgnoreCase(suffix)) {
            outputStream = new ZipOutputStream(wrapOutputStream(out));
            ((ZipOutputStream) outputStream).putNextEntry(new ZipEntry("result"));
        } else if (Const.SUFFIX_BZIP2.equalsIgnoreCase(suffix)) {
            outputStream = new BZip2CompressorOutputStream(wrapOutputStream(out));
        } else if (Const.SUFFIX_SNAPPY.equalsIgnoreCase(suffix)) {
            outputStream = new SnappyOutputStream(wrapOutputStream(out));
        } else if (Const.SUFFIX_LZO.equalsIgnoreCase(suffix)) {
            LzoAlgorithm algorithm = LzoAlgorithm.LZO1X;
            LzoCompressor compressor = LzoLibrary.getInstance().newCompressor(algorithm, null);
            outputStream = new LzoOutputStream(wrapOutputStream(out), compressor);
        } else if (Const.SUFFIX_LZMA.equalsIgnoreCase(suffix)) {
            outputStream = new LZMAOutputStream(wrapOutputStream(out), new LZMA2Options(), -1);
        } else if (Const.SUFFIX_LZ4.equalsIgnoreCase(suffix)) {
            LZ4Factory factory = LZ4Factory.fastestInstance();
            LZ4Compressor compressor = factory.fastCompressor();
            outputStream = new LZ4BlockOutputStream(wrapOutputStream(out), 8192, compressor);
        } else
            outputStream = out;
        return outputStream;
    }

    private static OutputStream wrapOutputStream(OutputStream outputStream) {
        OutputStream out = null;
        if (outputStream instanceof BufferedOutputStream) {
            out = outputStream;
        } else {
            out = new BufferedOutputStream(outputStream);
        }
        return out;
    }

    public abstract void moveTmpFileToProd(String tmpFile, String targetFile) throws IOException;
}
