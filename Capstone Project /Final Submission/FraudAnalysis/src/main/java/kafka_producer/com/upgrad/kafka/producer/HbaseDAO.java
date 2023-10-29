package kafka_producer.com.upgrad.kafka.producer;
import java.io.IOException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import kafka_producer.com.upgrad.kafka.producer.HbaseConnection;

public class HbaseDAO {

	// Function to get the data from HBase look up table 	
	@SuppressWarnings("deprecation")
	public static CardAnalysis getStats(String card_id) throws IOException {

	Admin hBaseAdmin1 = HbaseConnection.getHbaseAdmin();

	HTable table = null;
	try {
	Get g = new Get(Bytes.toBytes(card_id));
	table = new HTable(hBaseAdmin1.getConfiguration(), "Users_lookup_hb");

	Result result = table.get(g);
	
	byte[] uclByte= result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("ucl"));
	byte[] scoreByte = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("score"));
	byte[] postCodeByte= result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("postcode"));
	byte[] transactionDateByte= result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("transaction_dt"));
	
	String ucl = (uclByte != null) ? Bytes.toString(uclByte):"0";
	String score = (scoreByte != null) ? Bytes.toString(scoreByte):"-1";
	String postCode = (postCodeByte != null) ? Bytes.toString(postCodeByte):"0";
	String transactionDate = (transactionDateByte != null) ? Bytes.toString(transactionDateByte):"0";
	
    // reading data from look up table 
	return new CardAnalysis(ucl+","+score+","+postCode+","+transactionDate);

	} catch (Exception e) {

	e.printStackTrace();

	} finally {

	try {

	if (table != null)

	table.close();

	} catch (Exception e) {

	e.printStackTrace();

	}
	}

	System.out.println("unable to get the data from HBase");
	return null;

	}

	@SuppressWarnings("deprecation")
	public static boolean updatePostCodeTranDate(String card_id,String postCode, String transactionDate) throws IOException {

	Admin hBaseAdmin1 = HbaseConnection.getHbaseAdmin();

	HTable table = null;
	try {
	Put p = new Put(Bytes.toBytes(card_id));	
	table = new HTable(hBaseAdmin1.getConfiguration(), "Users_lookup_hb");
	p.add(Bytes.toBytes("cf"),
		      Bytes.toBytes("postcode"),Bytes.toBytes(postCode));
	p.add(Bytes.toBytes("cf"),
		      Bytes.toBytes("transaction_dt"),Bytes.toBytes(transactionDate));
	table.put(p);
	return true;

	} catch (Exception e) {

	e.printStackTrace();

	} finally {

	try {

	if (table != null)

	table.close();

	} catch (Exception e) {

	e.printStackTrace();

	}

	}
	return false;

	}




}
