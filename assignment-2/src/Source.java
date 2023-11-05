public class Source {
public static void main(String[] args) {  
 
	ElectionCount ec = new ElectionCount(); // creating object using direct addressing method
	ec.add(); //adding using direct addressing
	ec.findByVoterID("123456");
	ec.findCountByCandidateId("130");
	ElectionCountHash ech = new ElectionCountHash(); //creating object using hash table
	ech.add(); //adding using hash table
	ech.findByVoterID("123456"); 	
	ech.findCountByCandidateId("130");
	
	


	

}  

}