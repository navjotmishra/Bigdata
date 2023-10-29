package kafka_producer.com.upgrad.kafka.producer;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import scala.Serializable;


public class TranData implements Serializable {

	// Fields to hold the incoming transaction data 
	private static final long serialVersionUID = 5767679;
	private String card_id = null;
	private String member_id = null;
	private double amount = 0;
	private String postcode = null;
	private long pos_id = 0L;
	private String transaction_dt = null;
	
	
	// Constructor that sets the value to the fields using the fields from incoming transaction through KafkaSpark Streaming
	public TranData
	(String inCardId,String inMemberId,double inAmount,String inPostCode,long inPosId,String inTransactionDate) {
		
		this.card_id = inCardId;
		this.member_id = inMemberId;
		this.amount = inAmount;
		this.postcode = inPostCode;
		this.pos_id = inPosId;
		this.transaction_dt = inTransactionDate;
		
	}
	
	// Constructor that sets the value to the fields using the string from incoming transaction through KafkaSpark Streaming
		public TranData(String transactions) {
			System.out.println(transactions);
			this.card_id = transactions.split(",")[0];
			this.member_id = transactions.split(",")[1];
			this.amount = Double.parseDouble(transactions.split(",")[2]);
			this.postcode = transactions.split(",")[3];
			this.pos_id = Integer.parseInt(transactions.split(",")[4]);
			this.transaction_dt = transactions.split(",")[5];
		
		}
		
		// Constructor that sets the value to the fields using the string from incoming transaction through KafkaSpark Streaming
				public TranData(JSONObject js1) throws JSONException {
					//System.out.println(transactions);
					this.card_id = js1.get("card_id").toString();
					this.member_id = js1.get("member_id").toString();
					this.amount = Double.parseDouble((js1.get("amount").toString()) != null ? js1.get("amount").toString():"0");
					this.postcode = js1.get("postcode").toString() != null ? js1.get("postcode").toString():"0";
					this.pos_id = Long.parseLong((js1.get("pos_id").toString()) != null ? js1.get("pos_id").toString() : "0");
					this.transaction_dt = js1.get("transaction_dt").toString();
				
				}
				
				public TranData() {
					
				}
				public void setCard_id(String card_id) {
					this.card_id=card_id;
				}
					
				public void setMember_id(String member_id) {
					this.member_id=member_id;
				}
				
				public void setAmount(Double amount) {
					this.amount=amount;
				}
				
				public void setPostcode(String postcode) {
					this.postcode=postcode;
				}
				
				public void setPos_id(long pos_id) {
					this.pos_id=pos_id;
				}
				
				public void setTransaction_dt(String transaction_dt) {
					this.transaction_dt=transaction_dt;
				}
			
	public String getCard_id() {
		return this.card_id;
	}
		
	public String getMember_id() {
		return this.member_id;
	}
	
	public double getAmount() {
		return this.amount;
	}
	
	public String getPostcode() {
		return this.postcode;
	}
	
	public long getPos_id() {
		return this.pos_id;
	}
	
	public String getTransaction_dt() {
		return this.transaction_dt;
	}
	
	
	// Function to get all fields in a string format
	public String toString() {
		return this.card_id+","+this.member_id+","
			      +Double.toString(this.amount)+","
			      +this.postcode +","
			      +Long.toString(this.pos_id)+","
			      +this.transaction_dt;
	}
	
	// To display all fields
	public void show()
	{
		System.out.println(this.card_id+","+this.member_id+","
					      +Double.toString(this.amount)+","
					      +this.postcode + ","
					      +Long.toString(this.pos_id)+","
					      +this.transaction_dt);
	}
}
	


