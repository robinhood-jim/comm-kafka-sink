#
Kafka Sink
Consume Kafka data and sink to all kind of Source.
Do not use Kafka Connect,Prue Java.
Will add Kafka Connect Future.

Requirement
require JavaFrame https://github.com/robinhood-jim/JavaFrame component core common and hadooptool

Configuration
Configure with property File


brokerUrl      			//source kafka brokerUrl exmaple:localhost:9092
source_topic   			//input source topic (Now only support One topic)
groupId        			//Consume groupId

trans.partitonColumn	// format yyyyMMdd
trans.selectColumns		//select source Column
trans.insertColumns		//insert column Name
trans.insertColumnTypes	//insert column Type reference see class com.robin.core.base.util
trans.flushBasePath		//output path
trans.defaultFs 		//hadoop defaultFS config
trans.outputFileFormat	//file format support csv/json/avro/parquet/avro/protobuff
trans.compressType		//file compressType support gz bzip2 snappy lzo lz4
trans.outputSinkType    //output sink type support hdfs local sftp jdbc cassandra and so only
-----  jdbc relation config--------
target.dbType			//db Symbol (MySql Oracle SqlServer)
target.dbHost			
target.dbSchema
target.dbUser
target.dbPassword
target.insertColumns	//insert sql
