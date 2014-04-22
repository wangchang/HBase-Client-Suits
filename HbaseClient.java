import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.search.IndexSearcherClient;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.search.IndexAnalyzerNames;
import org.apache.hadoop.hbase.search.IndexMetadata;
import org.apache.hadoop.hbase.search.IndexSearchResponse;
import org.apache.hadoop.hbase.search.IndexMetadata.IndexFieldMetadata;
import org.apache.hadoop.hbase.search.IndexMetadata.IndexFieldType;
import org.apache.hadoop.hbase.search.IndexMetadata.IndexedHBaseColumn;
import org.apache.hadoop.hbase.search.IndexSearchResponse.IndexableRecord;
import org.apache.hadoop.hbase.search.IndexSearcher.SearchMode;
import org.apache.hadoop.hbase.search.query.expression.MatchQueryExpression;
import org.apache.hadoop.hbase.search.query.expression.QueryExpression;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseClient {
	private String tableName; // no use!作为函数的临时变量处理
	private String columnFamily; // should not be used!
	//private String indexname;
	//private String[] columns;
	private Configuration config;
	private HBaseAdmin admin;

	public HbaseClient() {
		this.config = HBaseConfiguration.create();
		//this.columns = new String[10];

		try {
			this.admin = new HBaseAdmin(new Configuration(this.config));
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		}

	}

	public void createIndexedTable(String table, String cfs, String iname,
			String cnames) {
		// 参数为 name,fm,c1,c2,c3的形式
		/*
		 * String strArray[] = args.split(","); this.tableName = strArray[0];
		 * this.columnFamily = strArray[1]; this.indexname = strArray[2]; for
		 * (int i = 3, j = 0; i < strArray.length; i++, j++) { this.columns[j] =
		 * strArray[i]; }
		 */
		System.out
				.println(String
						.format("\n[INFO]Create Indexed Table:%s,Column Families:%s,Index Name:%s,Indexed Columns:%s",
								this.tableName, cfs, iname, cnames));
		// System.out.println("Columns:" + Arrays.toString(this.columns));

		// 创建索引资料
		// 全文索引属性（总开关），可以认为是索引表
		IndexMetadata indexMetadata = new IndexMetadata();
		indexMetadata.setSearchMode(SearchMode.LastCommit);
		// 定义一个FIELD
		IndexFieldMetadata valueMeta1 = new IndexFieldMetadata();
		// 一个FIELD的设置
		valueMeta1.setName(Bytes.toBytes(iname));// TODO

		valueMeta1
				.setAnalyzerMetadataName(IndexAnalyzerNames.COMMA_SEPARATOR_ANALYZER);
		valueMeta1.setIndexed(true);
		valueMeta1.setTokenized(true);
		valueMeta1.setStored(true);
		valueMeta1.setType(IndexFieldType.STRING);
		// 这个FIELD需要和TABLE中哪个列簇中哪些列关联
		IndexedHBaseColumn column11 = new IndexedHBaseColumn(Bytes.toBytes(cfs)); // 列簇values
		for (String column : cnames.split(",")) {
			if (column != null)
				column11.addQualifierName(Bytes.toBytes(column));
		}
		// 把FIELD和这个列簇：列名关联起来
		valueMeta1.addIndexedHBaseColumn(column11);
		// 在索引表中加入这个索引关系
		indexMetadata.addFieldMetadata(valueMeta1);

		// 创建表，使用索引，指明列簇
		HTableDescriptor desc = new HTableDescriptor(this.tableName);
		desc.setIndexMetadata(indexMetadata);

		for (String cf : cfs.split(",")) {
			desc.addFamily(new HColumnDescriptor(cf));
		}

		try {
			this.admin.createTable(desc);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(String.format("[INFO]Create Table:%s Success!",
				this.tableName));

	}

	public boolean createNormalTable(String name, String cfs) {
		System.out.println(String.format(
				"\n[INFO]Create Normal Table:%s,Column Families:%s",
				this.tableName, cfs));
		HTableDescriptor desc = new HTableDescriptor(this.tableName);
		
		try{
			for (String cf : cfs.split(",")) {
				desc.addFamily(new HColumnDescriptor(cf));
			}
		}catch(Exception e){
			System.out.println("[ERROR]Column Families Args Wrong!");
			return false;
		}
	
		try {
			this.admin.createTable(desc);
			System.out.println(String.format("[INFO]Create Table:%s Success!",
					this.tableName));
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}	
		return true;	
	}

	// client.importTxtToTable(file,tablename,rowCons,columnCons,threadNum);
	public int importTxtToTable(String file, String tablename, String rowCons,
			String columnCons, String split) throws IOException,
			InterruptedException {
		int count = 0;
		this.tableName = tablename;
		HTable table = new HTable(this.config, Bytes.toBytes(tablename));
		table.setAutoFlush(false);

		if (table.isAutoFlush()) {
			System.out.println("Table AutoFlush Opened,Take Care!");
		}
		table.setWriteBufferSize(1024 * 1024 * 12);// 20M的缓存！
		// table.flushCommits();不用显式调用，超过缓冲后会自动调用。

		// 处理行键格式这种非常麻烦啊！第几个+第几个+第几个是行键
		String[] rowtemp = rowCons.split(",");
		int[] rowcons = new int[rowtemp.length];
		for (int i = 0; i < rowtemp.length; i++) {
			rowcons[i] = Integer.valueOf(rowtemp[i]);
		}
		// 处理列格式 ，更痛苦！family:qualifier1:1,family:qualifier2:2
		String[] colcons = columnCons.split(",");// family:qualifier1:1

		FileReader fr = new FileReader(file);
		BufferedReader br = new BufferedReader(fr);

		String line = null;
		long start = System.currentTimeMillis();
		while ((line = br.readLine()) != null) {
			String strArray[] = line.split("\\" + split);// TODO
			String rowKey = "";
			for (int i : rowcons) {
				rowKey += strArray[i - 1];
			}

			for (String col : colcons) {
				String[] coltemp = col.split(":");
				String qualifier = coltemp[1];
				String value = strArray[Integer.valueOf(coltemp[2]) - 1];
				Put put = new Put(Bytes.toBytes(rowKey));
				put.add(Bytes.toBytes(coltemp[0]), Bytes.toBytes(qualifier),
						Bytes.toBytes(value));
				table.put(put);
				// System.out.println("RowKey:"+rowKey+" ColFamily:"+coltemp[0]+" Qualifier:"+qualifier+" Value:"+value);
				count++;
				System.out.println("Write Count:" + count);
			}

		}
		// 写入完毕，进行刷新，关闭其他打开的文件描述
		long end = System.currentTimeMillis();
		System.out.println(String.format(
				"\n[INFO]TASK INFO: Time:%d ms,Table:%s,Count:%d rows", end - start,this.tableName,
				count));
		
		this.admin.flush(this.tableName);
		br.close();
		fr.close();
		table.close();
		// 返回一共写入多少行！
		return count;
	}

	public boolean showFileBase(String file) {
		File f = new File(file);
		int count = 0;
		System.out.println("[INFO]FileName:" + f.getPath() + " Size:"
				+ f.length() + " Bytes");
		
		FileReader fr;
		try {
			fr = new FileReader(file);
		} catch (FileNotFoundException e) {
			System.out.println("[ERROR]File Not Found!");
			return false;
		}
		
		BufferedReader br = new BufferedReader(fr);
		System.out.println("[INFO]First Five Lines of this file:");

		String line = null;
		try {
			while ((line = br.readLine()) != null && count < 5) {
				System.out.println(line);
				count++;
			}
		} catch (IOException e) {
			System.out.println("[ERROR]File Read Wrong!");
			return false;
		}
		try {
			br.close();
			fr.close();
		} catch (IOException e) {
			System.out.println("[ERROR]File Close Wrong!");
			return false;
		}
		return true;
		
	}

	public void scanFilterColumn(String args) throws IOException {
		int count = 0;
		String strArray[] = args.split(":");
		// this.tableName = strArray[0];
		this.columnFamily = strArray[0];
		String column = strArray[1];
		String columnValue = strArray[2];

		HTable table = new HTable(this.config, this.tableName);

		Filter filter = new SingleColumnValueFilter(
				Bytes.toBytes(this.columnFamily), Bytes.toBytes(column),
				CompareOp.EQUAL, Bytes.toBytes(columnValue));

		Scan scaner = new Scan();
		scaner.setFilter(filter);
		long start = System.currentTimeMillis();
		ResultScanner rs = table.getScanner(scaner);

		for (Result r : rs) { // Result代表一行，一行根据列名分为很多个KV！
			count++;
			for (KeyValue kv : r.raw()) {
				System.out
						.println(String
								.format("row:%s, family:%s, qualifier:%s, qualifiervalue:%s, timestamp:%s.",
										Bytes.toString(kv.getRow()),
										Bytes.toString(kv.getFamily()),
										Bytes.toString(kv.getQualifier()),
										Bytes.toString(kv.getValue()),
										kv.getTimestamp()));
			}
		}

		long end = System.currentTimeMillis();
		System.out.println(String.format(
				"\n[INFO]TASK INFO: Time:%d ms,Table:%s,Count:%d rows", end - start,this.tableName,
				count));
		table.close();

	}

	public void indexFilterColumn(String indexname,String args) throws Throwable {
		int count = 0;
		String strArray[] = args.split(":");
		// this.tableName = strArray[0];
		this.columnFamily = strArray[0];
		String column = strArray[1];
		String columnValue = strArray[2];

		HTable table = new HTable(this.config, this.tableName);
		IndexSearcherClient client = new IndexSearcherClient(table);
		// 定义索引查询表达式
		QueryExpression q = new MatchQueryExpression(indexname, columnValue);
		long start = System.currentTimeMillis();
		IndexSearchResponse resp = client.search(q, 0, 50, null);
		IndexableRecord[] records = resp.getRecords();

		// System.out.println(records.length);
		if (records != null) {
			for (IndexableRecord record : records) {
				Result r = record.getResult();
				count++;
				for (KeyValue kv : r.raw()) {
					System.out
							.println(String
									.format("row:%s, family:%s, qualifier:%s, qualifiervalue:%s, timestamp:%s.",
											Bytes.toString(kv.getRow()),
											Bytes.toString(kv.getFamily()),
											Bytes.toString(kv.getQualifier()),
											Bytes.toString(kv.getValue()),
											kv.getTimestamp()));
				}
			}
		}
		long end = System.currentTimeMillis();
		System.out.println(String.format(
				"\n[INFO]TASK INFO: Time:%d ms,Count:%d rows", end - start,
				count));
		table.close();
	}

	/**
	 * This should not be used!
	 * 
	 * @param table
	 * @throws TableNotFoundException
	 * @throws IOException
	 */
	public void showTableDescription(String table)
			throws TableNotFoundException, IOException {
		HTableDescriptor desc = this.admin.getTableDescriptor(Bytes
				.toBytes(table));
		System.out.println("[INFO] ALL:" + desc.toString());
		System.out.println("[INFO] Take Care Which Colums Will Be Indexed!");
		System.out.println("[INFO] TableName:" + desc.getNameAsString());
		// System.out.println(desc.getNameAsString());
		for (HColumnDescriptor a : desc.getColumnFamilies()) {
			System.out.println("[INFO] Column Families:" + a.getNameAsString());
		}
	}

	public void setTableName(String name) {
		this.tableName = name;
	}

	public boolean isTableAvaiable() throws IOException {
		if (this.admin.isTableAvailable(this.tableName)) {
			// System.out.println(String.format("\n[INFO]Table:%s Exists!",this.tableName));
			return true;
		} else {
			// System.out.println(String.format("\n[INFO]Table:%s Not Exists,Please Create Table First!",this.tableName));
			return false;
		}
	}

	public void showTableDescription() throws TableNotFoundException,
			IOException {
		HTableDescriptor desc = this.admin.getTableDescriptor(Bytes
				.toBytes(this.tableName));
		String tableDesc = desc.toString();
		System.out.println("\n================TABLE INFO==============");
		System.out.println("[INFO] Table Info:" + tableDesc);
		System.out.println("[INFO] TableName:" + desc.getNameAsString());
		// System.out.println(desc.getNameAsString());
		for (HColumnDescriptor a : desc.getColumnFamilies()) {
			System.out.println("[INFO] Column Families:" + a.getNameAsString());
		}
		if (tableDesc.indexOf("qualifierNames") > 0) {
			String temp = tableDesc.substring(tableDesc
					.indexOf("qualifierNames"));
			int start = temp.indexOf("[");
			int end = temp.indexOf("]");
			System.out.println("[INFO] Column Names:"
					+ temp.substring(start + 1, end));
			System.out
					.println("[INFO] Take Care Which Colums Will Be Indexed!");
		}
		System.out.println("============END TABLE INFO==============");
	}

	public void FreeClientResource() {
		try {
			this.admin.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

/*
 * public void compareIndexAndScan(String args) throws IOException, Throwable{
 * int count = 0; String strArray[] = args.split(","); this.tableName =
 * strArray[0]; this.columnFamily = strArray[1]; String column = strArray[2];
 * String columnValue = strArray[3];
 * 
 * HTable table = new HTable(this.config, this.tableName);
 * 
 * Filter filter = new SingleColumnValueFilter(Bytes.toBytes(this.columnFamily),
 * Bytes.toBytes(column), CompareOp.EQUAL, Bytes.toBytes(columnValue));
 * 
 * Scan scaner = new Scan(); scaner.setFilter(filter); ResultScanner rs =
 * table.getScanner(scaner);
 * 
 * IndexSearcherClient client = new IndexSearcherClient(table); QueryExpression
 * q = new MatchQueryExpression("field1", columnValue); IndexSearchResponse resp
 * = client.search(q, 0, 50, null); IndexableRecord[] records =
 * resp.getRecords();
 * 
 * //Result代表一行，一行根据列名分为很多个KV！
 * 
 * for (Result r : rs) { for(IndexableRecord record : records) { Result result =
 * record.getResult(); if(result.equals(rs)){ byte[] rowkey = result.getRow();
 * System.out.println(Bytes.toString(rowkey)); count ++; } } }
 * System.out.println(count); }
 */

/*
 * public boolean importLogToTable(String path) throws IOException,
 * InterruptedException{
 * 
 * System.out.println(String.format("Reading File:%s", path)); HTable table =
 * new HTable(this.config, Bytes.toBytes("besttable"));//TODO FileReader fr =
 * new FileReader(path); BufferedReader br = new BufferedReader(fr); int count =
 * 0; int flushflag = 0; List<Put> puts = new ArrayList<Put>();
 * 
 * String line = null; while((line = br.readLine()) != null){
 * //System.out.println(line); String strArray[]=line.split("\\|");
 * //System.out.println(strArray.length); //得到了一行的内容，准备写入数据库！ String rowKey =
 * strArray[1]+strArray[0]+strArray[5]; String remind = strArray[6]; String deal
 * = strArray[7]; //String stream = strArray[8];
 * //System.out.println(rowKey+" "+stream+" "+deal+" "+remind);
 * 
 * //写入数据库 //Put put1 = new Put(Bytes.toBytes(rowKey));
 * //put1.add(Bytes.toBytes(
 * this.columnFamily),Bytes.toBytes("stream"),Bytes.toBytes(stream));
 * //puts.add(put1);
 * 
 * Put put2 = new Put(Bytes.toBytes(rowKey));
 * put2.add(Bytes.toBytes("info"),Bytes
 * .toBytes("remind"),Bytes.toBytes(remind)); puts.add(put2);
 * 
 * Put put3 = new Put(Bytes.toBytes(rowKey));
 * put3.add(Bytes.toBytes("info"),Bytes.toBytes("deal"),Bytes.toBytes(deal));
 * puts.add(put3);
 * 
 * table.put(puts); puts.clear(); count++;
 * System.out.println(String.format("Success Put A row Num:%d!", count)); }
 * this.admin.flush(this.tableName); br.close(); fr.close(); table.close();
 * System.out.println(count); return true; }
 */
