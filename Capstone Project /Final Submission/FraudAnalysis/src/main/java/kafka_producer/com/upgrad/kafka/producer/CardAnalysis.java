package kafka_producer.com.upgrad.kafka.producer;
import kafka_producer.com.upgrad.kafka.producer.DistanceUtility;
import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.lang.Math;

public class CardAnalysis implements Serializable {

	// to get the columns from the HBase tables
	private static final long serialVersionUID = 1000000;
	private double  upperControlLimit = 0; //UCL
	private int score = 0; //Member Score
	private String postCode = null; //postal code
	private String transactionDate=null; //transaction date
	
	
	
	public CardAnalysis(String getdetails) throws ParseException
	{
		this.upperControlLimit = Double.parseDouble(getdetails.split(",")[0]);
		this.score = Integer.parseInt(getdetails.split(",")[1]);
		this.postCode = getdetails.split(",")[2];
		this.transactionDate = getdetails.split(",")[3];
		

	}
	
	public double getUCL() {
		return this.upperControlLimit;
	}
	
	public int getScore() {
		return this.score;
	}
	
	public String getPostCode() {
		return this.postCode;
	}
	
	public String	getTransactionDate() {
		return this.transactionDate;
	}
	// To get the combined output 
	public String toString() {
		return this.upperControlLimit+","+this.score+","+this.postCode+","+this.transactionDate+",";
	}	
	// UCL check
	public boolean checkUCL(double newAmount) {
		if(this.upperControlLimit > 0)
		{
			return this.upperControlLimit > newAmount ? true:false; 
		}
		else
			return true; 		
	}
	
	//member credit score check
	
	public boolean checkScore() {
		if(this.score > -1)
			return this.score > 200 ? true:false;
		else
			return true; 
	}
	
	// postal code check 
	public boolean checkPostCode(String newPostCode,String newTransactionDate) throws ParseException, NumberFormatException, IOException {
		SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
		DistanceUtility distUtility = new DistanceUtility();
		if(this.transactionDate != "0")
		{
			double dist = distUtility.getDistanceViaZipCode(newPostCode, this.postCode);
			double time = (Math.abs(formatter.parse(newTransactionDate).getTime() - formatter.parse(this.transactionDate).getTime())/ (1000*60*60));
			// if the speed < 1000 km/hr then GENUNIE else it is FRAUD	
			return dist/time < 1000 ? true:false; 
		}
		else if(newTransactionDate != null)
			return true; 
		else
			return false; 
		
	}
	
	// To combine results 
	public void show() {
		System.out.println("Upper_Control_Limit:"+this.upperControlLimit
						   +", Member_Score:"+this.score+", Last_Postal_Code:"+this.postCode
						   +", Last_Transaction_Date:"+this.transactionDate
						   );
	}
}




