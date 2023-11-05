import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;
public class ElectionCountHash {

//defining hash table
Hashtable <String,String> h = new Hashtable<String,String>();		
public void add() {	
	FileReader fr=null;
	try {
		// sample data is present in data file attached to project which can be modified
		fr = new FileReader("data");		
		BufferedReader br = new BufferedReader(fr);
		String text;
		while((text = br.readLine())!=null) {
			if(!text.isEmpty()){
				
				String[] itemValue = text.split(" "); //space is used as a delimiter split the candidate id and voterId
				h.put(itemValue[0], itemValue[1]);				
			}
		    }
		br.close();	
	} catch (FileNotFoundException e) {
	}catch (IOException e) {
	//Exception handling
		 System.out.println("Input File not found");
	}	
}
public void findByVoterID(String voterId) {
//getting the Key(VoterId) and returning the value(CandidateId)
String candidateId = h.get(voterId);
System.out.println("Candidate Id using Hash Table method is " + candidateId);
}

public void findCountByCandidateId (String candidateId)
{
	//Getting the value and counting the keys by parsing the hash table
	int count = 0 ;	
	Enumeration<String> e = h.keys();
	while(e.hasMoreElements()) {
		String key = e.nextElement();
		String candidateIdFromHash = h.get(key);
		if (candidateIdFromHash.equals(candidateId)){
		count++;
		}
	}
	System.out.println("Using Hash table method number of Votes received by CandidateId "+ candidateId + " is " + count);
	}
}