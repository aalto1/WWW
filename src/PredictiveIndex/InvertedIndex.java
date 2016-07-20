package PredictiveIndex;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.GZIPInputStream;


/**
 * Created by aalto on 6/24/16.
 */

public class InvertedIndex implements Serializable{

    private int distance;
    private int pointer = 0;
    private int [][] buffer;
    private int [] stats;                                       //1-numberofdocs,2-wordcounter,3-unique words
    private int doc;
    double start;
    double now = System.currentTimeMillis();
    static String path = "/home/aalto/IdeaProjects/PredictiveIndex/src/PredictiveIndex/tmp/ps.ser";
    private HashMap<String, Integer> termsMap;
    private HashMap<String, Integer[]> map;
    private HashMap<String, Integer> docsMap;
    private HashMap<Integer,Integer> freqTermDoc;
    private File invertedIndex;

    public InvertedIndex(int distance, String colletion) {
        // Class constructor

        this.stats = new int[5];
        this.map = new HashMap<>();
        this.freqTermDoc = new HashMap<>();
        this.termsMap = new HashMap<>();
        this.docsMap = new HashMap<>();
        this.doc = 0;
        this.distance = distance;
        this.buffer = new int [20000000][5];         //1G buffer
    }

    // *****************************************************************************************
    // 1TH PHASE - GET METADATA
    // *****************************************************************************************

    //PROCESS DATA TO GET INFO *****************************************************************************************

    public void getCollectionMetadata(String folder) throws IOException {
        doc = 0;
        start =  System.currentTimeMillis();
        for(File file : new File(folder).listFiles()){
            ArrayList<LinkedList> fowardIndex = new ArrayList<LinkedList>();
            System.out.println("Now processing file: "+ file);
            GZIPInputStream gzInputStream=new GZIPInputStream(new FileInputStream(file));
            DataInputStream inStream=new DataInputStream(gzInputStream);
            WarcRecord thisWarcRecord;
            while ((thisWarcRecord=WarcRecord.readNextWarcRecord(inStream))!=null) {
                if (thisWarcRecord.getHeaderRecordType().equals("response")) {
                    //processWARCRecord((String []) recordData.getLast());
                    LinkedList<Object> recordData = thisWarcRecord.getCleanRecord();
                    this.processWARCRecord((String []) recordData.getLast(), (String) recordData.getFirst());
                    fowardIndex.add(recordData);
                    doc++;
                }
                if(doc%1000==0) System.out.println(doc);
            }
            inStream.close();
            serialize(fowardIndex,file.getName());


        }this.hash2CSV();

    }

    public void processWARCRecord(String [] words, String title){
        this.docsMap.putIfAbsent(title, this.stats[0]);
        HashMap<String, Integer> auxHash= new HashMap<>();
        for(String word : words){
            if(auxHash.putIfAbsent(word,1)==null){
                if(this.termsMap.putIfAbsent(word,stats[2])==null){
                    this.stats[2]++;
                }else{
                    this.freqTermDoc.merge(getWId(word), 1, Integer::sum);
                }

            }
        }
        this.stats[0]++;
        this.stats[1] += words.length;
    }

    //STORE INFO *******************************************************************************************************

    public void fetchGInfo() throws FileNotFoundException {
        //this methods returns the global statistics about the dataset if already processed.

        Scanner scanner = new Scanner(new File("gstats.txt"), "UTF-8");
        for(int k=0; scanner.hasNext(); k++){
            this.stats[k] = Integer.valueOf(scanner.next());
        }
    }

    public void dumpGInfo() throws FileNotFoundException {
        //this methods saves the global statistics about the whole dataset.

        try (FileWriter fw = new FileWriter("gstats.txt", true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {
            for(int k = 0; k<this.stats.length; k++) {
                out.println(this.stats[k]);
            }
        } catch (IOException e) {
            System.out.println("No dump, No party!");
            System.exit(1);
        }
    }

    public void hash2CSV() throws IOException {
        //this function save the map term-freq to disk

        Iterator it = this.freqTermDoc.entrySet().iterator();
        try(FileWriter fw = new FileWriter("termsOccurence.csv", true);
            BufferedWriter bw = new BufferedWriter(fw);
            PrintWriter out = new PrintWriter(bw)) {
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry)it.next();
                String aux = pair.getKey() + "," + pair.getValue();
                out.println(aux);
                it.remove(); // avoids a ConcurrentModificationException
            }
        }
    }

    //EXTRA ************************************************************************************************************

    // *****************************************************************************************
    // 2ND PHASE - BUILD INVERTED INDEX
    // *****************************************************************************************

    public void naturalSelection(){
        System.out.println("TIME TO CLEAN. Processed docs: " + doc);
        now =System.currentTimeMillis();
        this.sortBuffer();

        int auxValue = this.buffer[0][0];
        int shifter = 0;
        int startPointer = 0;
        int lLen = 0;
        for(int k=0; k<this.buffer.length; k++){
            if(this.buffer[k][0] != auxValue | k== this.buffer.length-1){
                lLen = (k-startPointer);
                //System.out.println("ciao");
                for(int k2 = startPointer; k2 < startPointer+lLen*0.2; k2++){
                    this.buffer[k2-shifter][0] = this.buffer[k2][0];
                    this.buffer[k2-shifter][1] = this.buffer[k2][1];
                    this.buffer[k2-shifter][2] = this.buffer[k2][2];
                    this.buffer[k2-shifter][3] = this.buffer[k2][3];
                }
                shifter += lLen*0.8;
                startPointer = k;
                auxValue = this.buffer[k][0];
            }

        }

        System.out.println("Sorting Time: " + (System.currentTimeMillis()-now) + "ms. Processing Time:" + 1000/((System.currentTimeMillis()-start)/doc) +"doc/sec");
    }

    public void sortBuffer(){
        Arrays.sort(this.buffer, new Comparator< int []>()
        {
            @Override
            public int compare(int[] int1, int[] int2)
            {
                //if we have the same doc ids sort them based on the bm25
                if (int1[0]==int2[0]) {
                    if(int1[1]==int2[1]){
                        return Integer.compare(int1[3]+int1[4],int2[3]+int2[4]);
                    }else return Integer.compare(int1[1],int2[1]);
                }else return Integer.compare(int1[0],int2[0]);
            }
        });
    }

    public int getBM25(int id, int docLen, int f) {
        // global statistics for BM25
        int N = this.stats[0];
        int n = this.freqTermDoc.get(id);
        double avg  = this.stats[1]/this.stats[0];
        double k = 1.6;
        double b = 0.75;
        double IDF = java.lang.Math.log(N - n + 0.5 / n + 0.5);
        double BM25 = (IDF * f * k + 1)/(f + k * (1 - b*docLen/avg));
        return (int) BM25;
    }

    public void map2Buffer(HashMap<Integer, Integer> freq, HashSet<String> pairs, int docSize, int docId){
        //We want to avoid huge hashmaps and for each warc document we flush it in a static data structure

        String pair;
        String [] words;
        Iterator it = pairs.iterator();
        while (it.hasNext()) {
            pair = it.next().toString();
            words = pair.split("-");
            int e1 = getWId(words[0]);
            int e2 = getWId(words[1]);
            this.buffer [pointer][0] = e1;
            this.buffer [pointer][1] = e2;
            this.buffer [pointer][2] = docId;
            this.buffer [pointer][3] = getBM25(e1, docSize, freq.get(e1));
            this.buffer [pointer][4] = getBM25(e2, docSize, freq.get(e2));
            if(pointer == buffer.length-1){
               naturalSelection();
               pointer = (this.buffer.length/100)*20;
            }else pointer++;
            it.remove(); // avoids a ConcurrentModificationException
            }
        }

    public void bufferedIndex(String [] words, String title){
        HashMap<Integer, Integer> auxFreq = new HashMap<>();
        HashSet<String> auxPair = new HashSet<>();
        for(int wIx = 0; wIx < words.length; wIx++){
            if(wIx < words.length-this.distance){
                for(int dIx = 1; dIx<this.distance; dIx++){
                    String [] pair2Sort = {words[wIx],words[wIx+dIx]};
                    Arrays.sort(pair2Sort);
                    String pair = pair2Sort[0]+"-"+pair2Sort[1];
                    auxPair.add(pair);
                }
            }if(auxFreq.putIfAbsent((getWId(words[wIx])),1)==null){
                this.freqTermDoc.merge(getWId(words[wIx]), 1, Integer::sum);
            }
        }
        this.map2Buffer(auxFreq, auxPair, words.length ,getDId(title));
    }

    public int getWId(String word){
        return this.termsMap.get(word);
    }

    public int getDId(String word){
        return this.docsMap.get(word);
    }



    // STORE INVERTED INDEX ********************************************************************************************

    public void serialize(Object e, String file){

        try {
            FileOutputStream fileOut =
                    new FileOutputStream(path);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(e);
            out.close();
            fileOut.close();
            System.out.printf(path);
        }catch(IOException i)
        {
            i.printStackTrace();
        }
    }

    public ArrayList deserialize(File file){
        ArrayList<LinkedList> e = null;
        try
        {
            FileInputStream fileIn = new FileInputStream(file);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            e = (ArrayList<LinkedList>) in.readObject();
            in.close();
            fileIn.close();
            return e;
        }catch(IOException i)
        {
            i.printStackTrace();
            return null;
        }catch(ClassNotFoundException c)
        {
            System.out.println("Object not found");
            c.printStackTrace();
            return null;
        }
    }

    public static InvertedIndex deserPS(File file){
        InvertedIndex e = null;
        try
        {
            FileInputStream fileIn = new FileInputStream(file);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            e = (InvertedIndex) in.readObject();
            in.close();
            fileIn.close();
            return e;
        }catch(IOException i)
        {
            i.printStackTrace();
            return null;
        }catch(ClassNotFoundException c)
        {
            System.out.println("Object not found");
            c.printStackTrace();
            return null;
        }
    }



    public void dumpBuffer(String name){
        // Save the partial inverted index (a.k.a buffer) to disk

        String prefix;
        StringBuilder toFlush = new StringBuilder();
        for(int [] entry : this.buffer){
            prefix = "";
            for(int num : entry){
                toFlush.append(prefix+num);
                prefix= ",";
            }
            toFlush.append("\n");
        }
        File file = new File("dumps/"+name+".txt");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            writer.append(toFlush);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void dumpBuffer2(String name){
        // Save the partial inverted index (a.k.a buffer) to disk

        this.sortBuffer();
        StringBuilder toFlush = new StringBuilder();
        for(int [] entry : this.buffer) {
            toFlush.append(entry[0] + "," + entry[1] + "," + (entry[3] + entry[4]) + "," + entry[2] + "\n");
        }
        File file = new File("dumps/"+name+".txt");

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            writer.append(toFlush);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void buildIndex(){
        doc = 0;
        start =  System.currentTimeMillis();
    String folder = "/home/aalto/IdeaProjects/PredictiveIndex/src/PredictiveIndex/tmp";
        for(File file : new File(folder).listFiles()){
            ArrayList<LinkedList<Object>> fowardIndex= deserialize(file);
            LinkedList<Object> aux;
            Iterator it = fowardIndex.iterator();
            start =  System.currentTimeMillis();
            while(it.hasNext()){
                aux = (LinkedList) it.next();
                this.bufferedIndex((String []) aux.getLast(), (String) aux.getFirst());
                doc++;
            }
        dumpBuffer2(file.getName());
        pointer = 0;
        }
    }

    public static void main(String [] args) throws IOException {
        String folder = "/home/aalto/IdeaProjects/PredictiveIndex/data/test";
        InvertedIndex ps;
        if (Files.exists(Paths.get(path))) {
            System.out.println("Deserializing Predictive Inverted...");
            ps = deserPS(new File(path));
            System.out.println("Predictive Index Deserialized");
        }else {
            ps = new InvertedIndex(2);
            ps.getCollectionMetadata(folder);
            ps.buffer = null;
            ps.serialize(ps, path);
        }
        ps.buildIndex();//*/
        //ImpactModel.buildImpactModel();
    }
}


/*
    Integer t1 = -1;
        Integer t2 = -1;
        String prefix="";
        StringBuilder toFlush = new StringBuilder();
        for(int [] entry : this.buffer){
            if(entry[0]!=t1 | entry[1]!=t2){
                toFlush.append(prefix+entry[0]+","+entry[1]+","+entry[2]);
                prefix="\n";
            }else toFlush.append(","+entry[2]);
            t1=entry[0];
            t2=entry[1];
        }


        *********************************************************
        *
        *
        * StringBuilder toFlush = new StringBuilder();
        for(int [] entry : this.buffer){
            toFlush.append(entry[0]+","+entry[1]+","+(entry[3]+entry[4])+","+entry[2]+"\n");

        }
 */