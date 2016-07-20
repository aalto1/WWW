package PredictiveIndex;

import javax.ws.rs.core.MultivaluedHashMap;
import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static PredictiveIndex.HtmlpageCleaner.startProcess;


public class PairwiseInvertedIndex {
	int d;
	int docNum;
	HashMap<Integer, Integer> pairCount;
	MultivaluedHashMap<Integer, Integer> invertedIndex;
	HashMap<Integer, Boolean> noDuplicates;

	PairwiseInvertedIndex(){
		pairCount = new HashMap<>();
		invertedIndex = new MultivaluedHashMap<>();
		noDuplicates = new HashMap<>();
		d = 10;
		docNum=0;
	}
	
	public void addDoc2Index(String [] words, int title){
		for(int wIx = 0; wIx < words.length-d; wIx++){
			for(int dIx = 1; dIx<d-1; dIx++){
				String[] pair2Sort = {words[wIx],words[wIx+dIx]};
				Arrays.sort(pair2Sort);
				//List<String> pairKey = Collections.unmodifiableList(Arrays.asList(pair2Sort));
				int pairKey = (pair2Sort[0]+pair2Sort[1]).hashCode();
				if(noDuplicates.containsKey(pairKey)){
					this.pairCount.put(pairKey, pairCount.get(pairKey)+1);


				}else{
					this.invertedIndex.add((pair2Sort[0]+pair2Sort[1]).hashCode(), title);
					this.noDuplicates.put(pairKey, true);
					this.pairCount.put(pairKey, 1);

					//System.out.println("["+pairKey.get(0) + "][" + pairKey.get(1)+"]");
				}
			}
		}
	}

	
	public void documentParser(File inputWarcFile) throws FileNotFoundException, IOException{
        // open our gzip input stream
        GZIPInputStream gzInputStream=new GZIPInputStream(new FileInputStream(inputWarcFile));

        // cast to a data input stream
        DataInputStream inStream=new DataInputStream(gzInputStream);

        // iterate through our stream
        WarcRecord thisWarcRecord;
        //System.out.println(WarcRecord.readNextWarcRecord(inStream));
        while ((thisWarcRecord=WarcRecord.readNextWarcRecord(inStream))!=null) {
            // see if it's a response record
            if (thisWarcRecord.getHeaderRecordType().equals("response")) {
                // it is - create a WarcHTML record
                WarcHTMLResponseRecord htmlRecord=new WarcHTMLResponseRecord(thisWarcRecord);
                // get our TREC ID and target URI
                String thisTRECID=htmlRecord.getTargetTrecID().replace("-","");
                String thisTargetURI=htmlRecord.getTargetURI();
                String aux = htmlRecord.getRawRecord().getContentUTF8();
                String [] text = startProcess(aux).split(" ");
                this.addDoc2Index(text, Integer.parseInt(thisTRECID.substring(11,thisTRECID.length())));
                if (this.docNum % 100 == 0){
                    System.out.println(this.docNum + " Documents Processed");
                    //System.out.println(Runtime.getRuntime().freeMemory());
                }
                this.docNum++;
            }
			this.noDuplicates = new HashMap<>();
        }

        inStream.close();
    }


	public static void main(String[] args) throws IOException{
		PairwiseInvertedIndex index = new PairwiseInvertedIndex();
		String folder = System.getProperty("user.dir") + "/data";
		System.out.println(folder);
		for(File file : new File(folder).listFiles()){
			System.out.println("Now processing file: "+file);
			index.documentParser(file);
		}
		List<String> pair = Arrays.asList("Endorsement", "Security");
		System.out.println(index.pairCount.get(pair));
		System.out.println(index.invertedIndex.get(pair));
	}
}