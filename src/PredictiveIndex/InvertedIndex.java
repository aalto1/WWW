package PredictiveIndex;

import PredictiveIndex.tmp.AppendingObjectOutputStream;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.GZIPInputStream;


/**
 * Created by aalto on 6/24/16.
 */

public class InvertedIndex implements Serializable {

    static final String path = "/home/aalto/IdeaProjects/PredictiveIndex/src/PredictiveIndex/tmp";
    static final String tPath = path + "/termMap";
    static final String fPath = path + "/freqMap";
    static final String docMapPath = path + "/docMapPath";
    static final String sPath = path + "/stats";
    static final String dPath = path + "/dump";
    static final String fIndexPath = path + "/FI";
    static final String ser = ".ser";
    static double start;
    static double now;
    static double deserializeTime = 0;
    static final int min = 50;
    static final int max = 500;
    static int saved =0;



    //private AppendingObjectOutputStream invertedIndexFile;
    private ObjectOutputStream invertedIndexFile;
    private ObjectOutputStream forwardIndexFile;
    final private int distance = 5;
    private int pointer = 0;
    private int[][] buffer;
    private int[] stats;                                       //1-numberofdocs,2-wordcounter,3-unique words
    private int doc;
    private HashMap<String, Integer> termsMap;
    private HashMap<String, Integer> docsMap;
    private HashMap<Integer, Integer> freqTermDoc;

    public InvertedIndex() throws IOException {
        // Class constructor

        this.doc = 0;
        this.termsMap = new HashMap<>();
        this.freqTermDoc = new HashMap<>();
        this.docsMap = new HashMap<>();
        this.stats = new int[5];
        this.forwardIndexFile = null;
        this.invertedIndexFile = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(dPath + "/II.dat", true)));
    }

    public InvertedIndex(HashMap<String, Integer> termsMap, HashMap<Integer, Integer> freqTermDoc, HashMap<String, Integer> docsMap, int[] stats) throws IOException {
        // Class constructor


        this.doc = 0;
        //this.buffer = new int[10000000][4];
        this.termsMap = termsMap;
        this.freqTermDoc = freqTermDoc;
        this.docsMap = docsMap;
        this.stats = stats;
        this.invertedIndexFile = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(dPath + "/II.dat", true)));

    }


    // *****************************************************************************************
    // 1TH PHASE - GET METADATA
    // *****************************************************************************************

    //PROCESS DATA TO GET INFO *****************************************************************************************

    public void getCollectionMetadata(String data) throws IOException {
        doc = 0;
        int collection = 0;
        start = System.currentTimeMillis();
        for (File file : new File(data).listFiles()){
            this.forwardIndexFile = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(fIndexPath + "/forwardIndex" + doc + ".dat", true)));
            System.out.println("Now processing file: " + file);
            GZIPInputStream gzInputStream = new GZIPInputStream(new FileInputStream(file));
            DataInputStream inStream = new DataInputStream(gzInputStream);
            WarcRecord thisWarcRecord;
            while ((thisWarcRecord = WarcRecord.readNextWarcRecord(inStream)) != null) {
                if (thisWarcRecord.getHeaderRecordType().equals("response")) {
                    String [] recordData = thisWarcRecord.getCleanRecord2();
                    this.processWARCRecord(recordData, recordData[recordData.length-1]);
                    doc++;
                }
                if (doc % 1000 == 0) System.out.println(doc);
            }
            collection++;
            inStream.close();
            this.forwardIndexFile.close();
            System.out.println("New Processing Time:" + (doc / (System.currentTimeMillis() - start)) * 1000 + " doc/s");
            //System.out.println(saved);
            //if(collection ==2) break;
        }
        this.savePSMetadata();
    }

    public void processWARCRecord(String[] words, String title) throws IOException {
        /*this function process the single wrac files */

        int docID = this.stats[0];
        this.docsMap.putIfAbsent(title, docID);
        int [] intWords = new int[words.length+1];
        intWords[0] = this.stats[0];                                        //frist element is the doc title
        HashMap<Integer, Integer> position = new HashMap<>();
        ArrayList<Integer> singeDocStats = new ArrayList<>();
        int pos;
        int uniqueWords = 0;

        int termID;
        for (int k = 0; k<words.length-1; k++) {
            termID = putWordIfAbstent(words[k]);
            intWords[k+1] = termID;
            //We use and auxiliary hashmap to store the frequency and then we convert it to an array

            if (position.putIfAbsent(termID, 1) == null){
                this.freqTermDoc.merge(termID, 1, Integer::sum);
            }else{
                position.merge(termID, 1, Integer::sum);
            }
        }
        this.forwardIndexFile.writeObject(intWords);
        this.forwardIndexFile.writeObject(hashMapToArray(position));
        this.stats[0]++;
        this.stats[1] += words.length;
    }

    public static int [] hashMapToArray(HashMap<Integer,Integer> map){
        /*This function convert an HashMap to an 1d array with the dimension doubled respect to the key-value pairs
        * with a value bigger than one*/

        int [] array = new int[map.size()*2];
        int value;
        int k = 0;
        for(int key : map.keySet()){
            value = map.get(key);
            if((value)>1){                          //becasue we want to reduce the overhead (20% space saved per dump)
                array[k*2] = key;
                array[k*2+1] = value;
                k++;
            }
        }
        //saved += (map.size()-(k));
        return Arrays.copyOf(array, k*2+1);
    }

    public static HashMap<Integer, Integer> arrayToHashMap(int [] array){
        /*This function converst an 1d array to an hashmap. We use this function to get the term freq. for a specific
        * term within a doc*/

        HashMap<Integer, Integer> map = new HashMap<>();
        for(int k = 0; k<array.length; k+=2){
            map.put(k, k+1);
        }
        return map;
    }


    public int putWordIfAbstent(String word){
        /*This function checks if the term is in our term-termID map. If a term is present it return the termID, if not
        * adds a new entry to the hashmap. The termID is the number of unique words that we have encountered up to that
        * moment.*/

        int termID;
        try{
            termID = getWId(word);
        }catch(NullPointerException e){               // add new document in our dictionary
            termID = stats[2];
            this.stats[2]++;
            this.termsMap.put(word, termID);
            this.freqTermDoc.put(termID, 0);
        }
        return termID;
    }

    //SAVE ************************************************************************************************************

    public void savePSMetadata() {
        /**/

        serialize(this.termsMap, tPath);
        serialize(this.freqTermDoc, fPath);
        serialize(this.docsMap, docMapPath);
        serialize(this.stats, sPath);

    }

    public static void serialize(Object e, String file) {
        /**/

        try {
            FileOutputStream fileOut = new FileOutputStream(new File(file + ser));
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(e);
            out.close();
            fileOut.close();
        } catch (IOException i) {
            i.printStackTrace();
        }
    }

    public static Object deserialize(String file) {
        /**/

        Object e = null;
        try {
            FileInputStream fileIn = new FileInputStream(file);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            e = in.readObject();
            in.close();
            fileIn.close();
            return e;
        } catch (IOException i) {
            i.printStackTrace();
            return null;
        } catch (ClassNotFoundException c) {
            System.out.println("Object not found");
            c.printStackTrace();
            return null;
        }
    }

    // *****************************************************************************************
    // 2ND PHASE - BUILD INVERTED INDEX
    // *****************************************************************************************

    public void buildIndex() throws IOException {
        /* To build the inverted index we retrive the collection of document that have been serialized in a list of
        * array strings. For each of the element of this list, which is a document, we call buffered index which process
        * its content and give add it to the buffered index (temporary has map that will be dumped in the array)*/

        doc = 0;
        start = System.currentTimeMillis();
        this.buffer = new int[20000000][4];
        for (File file : new File(fIndexPath).listFiles()) {
            System.out.println("Processing WRAC: " + file.getName());
            this.readFromBinaryFile(file.getAbsolutePath());
        }
        this.invertedIndexFile.close();
    }

    public void readFromBinaryFile (String filename) {
        /*For each document we retrieve the document itself, its title and statistics*/

        File file = new File(filename);

        if (file.exists()) {
            ObjectInputStream ois = null;
            try {
                ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(filename)));
                while (true) {
                    int [] intWords = (int[])  ois.readObject();    //title + document
                    HashMap<Integer,Integer> docTermsStats = arrayToHashMap((int[])  ois.readObject()); //docstats
                    this.bufferedIndex(intWords, intWords[0], docTermsStats);
                    doc++;
                }
            }catch (EOFException e) {
            }catch (Exception e) {
                e.printStackTrace();
            }finally {
                try {
                    if (ois != null) ois.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static int getTermFreq(HashMap<Integer,Integer> docStat, int termID){
        /*We try to get the frequency of a term using the termID-freq hashmap. If a NullPointerException error is raised
        * than it mean that the term freq is just 1.*/

        try{
            return docStat.get(termID);
        }catch (NullPointerException e){
            return 1;
        }
    }

    public void bufferedIndex(int[] words, int title, HashMap<Integer,Integer> docStat) throws IOException {
        /* For each document we take the pairs between documents within a distance. We add each entry to a buffer and
        * compute the BM25 for that specific term-pair*/

        int f1;
        int f2;
        HashSet<Integer> auxPair = new HashSet<>();
        for (int wIx = 1; wIx < words.length - this.distance; wIx++) {
            for (int dIx = wIx+1; dIx < wIx + this.distance; dIx++) {
                int [] pair = {words[wIx], words[dIx]};
                Arrays.sort(pair);
                f1 = getTermFreq(docStat, pair[0]);
                f2 = getTermFreq(docStat, pair[1]);
                if(auxPair.add(pair[0]+pair[1]) == true) {
                    this.add2Buffer(pair[0], pair[1], f1, f2, words.length, title);
                }
            }
        }
    }

    public void add2Buffer(int e1, int e2, int f1, int f2, int docSize, int docId) throws IOException {
        /*This function add an entry to our buffer. If the buffer is full it calls the naturalSelection() which will
        * sort the buffer and prune it keeping just the best ones based on BM25*/

        this.buffer[pointer][0] = e1;
        this.buffer[pointer][1] = e2;
        this.buffer[pointer][2] = getBM25(e1, docSize, f1) + getBM25(e2, docSize, f2);
        this.buffer[pointer][3] = docId;
        if (pointer == buffer.length - 1){
            naturalSelection();
            pointer = 0;
        }else pointer++;
    }

    public int getBM25(int id, int docLen, int f) {
        /*global statistics for BM25*/
        int N = this.stats[0];
        int n = this.freqTermDoc.get(id);
        double avg = this.stats[1] / this.stats[0];
        double k = 1.6;
        double b = 0.75;
        double IDF = java.lang.Math.log(N - n + 0.5 / n + 0.5);
        double BM25 = (IDF * f * k + 1) / (f + k * (1 - b * docLen / avg));
        return (int) BM25;
    }

    public int getWId(String word) {
        return this.termsMap.get(word);
    }

    public int getDId(String word) {
        return this.docsMap.get(word);
    }

    public static double boundedKeep(int diff) {
        /*we want to preserve the short lists and cut the long ones. The bounds are min and max. We have four cases
        * 20% > max  ->    keep max
        * 20% > min   ->    keep 20%
        * diff > min  ->    keep min
        * diff < min  ->    keep diff */

        return (diff * 0.2 > max) ? max : (diff * 0.2 > min) ? diff * 0.2 : (diff > min) ? min : diff;
    }

    public void naturalSelection() throws IOException {
        /*We want to keep just the 20% of each posting list. If the remainig list is bigger or smaller of min and max
        * we protect or truncate it.
        * WE CAN IMPROVE THIS FUNCTION BY USING AN HASMAP THAT SAVES FOR EACH PAIR HOW MANY ENTRIES IT HAS. I DON'T KNOW
        * HOW MUCH SPACE IT WOULD REQUIRE. IN THIS WAY WE COULD SCAN THE ARRAY JUST ONE TIME*/

        System.out.println("TIME TO CLEAN. Processed docs: " + doc);
        this.sortBuffer();
        now = System.currentTimeMillis();
        int[] nowPosting = this.buffer[0];
        int startPointer = 0;
        int keep;
        int [] aux = new int[4];
        for (int k = 0; k < this.buffer.length; k++) {
            if (this.buffer[k][0] != nowPosting[0] | this.buffer[k][1] != nowPosting[1] | k == this.buffer.length - 1) {
                keep = (int) boundedKeep(k - startPointer);
                for (int k2 = startPointer; k2 < startPointer + keep; k2++){
                    aux[0] = this.buffer[k2][0];
                    aux[1] = this.buffer[k2][1];
                    aux[2] = this.buffer[k2][2];
                    aux[3] = this.buffer[k2][3];
                    //System.out.println(aux[0]+","+aux[1]+","+aux[2]+","+aux[3]);
                    this.invertedIndexFile.writeObject(aux);
                }
                startPointer = k;
                nowPosting = this.buffer[startPointer];
            }
        }
        System.out.println("Flush Time:" + (System.currentTimeMillis() - now) + "ms");
        System.out.println("Processing Time:" + (doc / (System.currentTimeMillis() - start)) * 1000 + " doc/s");
    }

    public void sortBuffer() {
        /*Sorting  function.
        * WE CAN IMPROVE THIS ASSINING AN UNIQUE IDENTIFIER TO EACH PAIR OF TERMS*/

        now = System.currentTimeMillis();
        Arrays.sort(this.buffer, new Comparator<int[]>() {
            @Override
            public int compare(int[] int1, int[] int2) {
                //if we have the same doc ids sort them based on the bm25
                if (int1[0] == int2[0]) {
                    if (int1[1] == int2[1]) {
                        return Integer.compare(int1[2], int2[2]) * -1;
                    } else return Integer.compare(int1[1], int2[1]);
                } else return Integer.compare(int1[0], int2[0]);
            }
        });
        System.out.println("Sorting Time: " + (System.currentTimeMillis() - now) + "ms");
    }
}






