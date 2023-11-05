import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
class ElectionCount{
	int range = 100000;
	//Array for direct addressing
	Record[] record = new Record[1000000];	
public void add() {	
	FileReader fr=null;
	try {
		// sample data is present in data file attached to project which can be modified
		fr = new FileReader("data");		
		BufferedReader br = new BufferedReader(fr);
		String text;
		while((text = br.readLine())!=null) {
			Record item = new Record(); 		
			if(!text.isEmpty()){		
				String[] itemValue = text.split(" "); //space is used as a delimiter split the candidate id and voterId				
				int index=Integer.parseInt(itemValue[0])-range;
				item.setVoter_Id((itemValue[0]));  
				item.setCandidate_Id((itemValue[1])); 
				record[index] = item;				
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
// getting the voterId and displaying the candidateId based on the index of the array
int index = 0;
index = Integer.parseInt(voterId) - range;
Record rec = record[index];
if(rec !=null) {
	System.out.println("Candidate Id using Direct addressing method is " + rec.getCandidate_Id()); 
}else {
	System.out.println("Record not found"); 
}
}

public void findCountByCandidateId (String candidate_Id) {
//Getting the candidateId and returning the count of votes	
	int count=0;
	for(Record rec:record) {
		if (rec!= null) {
		String candidateId= rec.getCandidate_Id();
		if(candidateId.equals(candidate_Id)) {			
			count++;
		}	
		}
	}	
	System.out.println("Using Direct Addressing method Number of Votes received by CandidateId "+ candidate_Id +" is "+ count);
	}	
}