package PredictiveIndex;

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
    static final String dPath = path + "/dumps";
    static final String fIndexPath = path + "/fowardIndex";
    static final String ser = ".ser";
    static double start;
    static double now = System.currentTimeMillis();

    final private int distance = 5;
    private int pointer = 0;
    public int[][] buffer;
    private int[] stats;                                       //1-numberofdocs,2-wordcounter,3-unique words
    private int doc;
    private HashMap<String, Integer> termsMap;
    private HashMap<String, Integer> docsMap;
    private HashMap<Integer, Integer> freqTermDoc;
    private File invertedIndex;

    public InvertedIndex() {
        // Class constructor

        this.stats = new int[5];
        this.freqTermDoc = new HashMap<>();
        this.termsMap = new HashMap<>();
        this.docsMap = new HashMap<>();
        this.doc = 0;
        this.buffer = new int[10000000][5];         //1G buffer
    }

    public InvertedIndex(HashMap<String, Integer> termsMap, HashMap<Integer, Integer> freqTermDoc, HashMap<String, Integer> docsMap, int[] stats) {
        // Class constructor


        this.doc = 0;
        this.buffer = new int[10000000][5];
        this.termsMap = termsMap;
        this.freqTermDoc = freqTermDoc;
        this.docsMap = docsMap;
        this.stats = stats;

    }



    // *****************************************************************************************
    // 1TH PHASE - GET METADATA
    // *****************************************************************************************

    //PROCESS DATA TO GET INFO *****************************************************************************************

    public void getCollectionMetadata(String data) throws IOException {
        doc = 0;
        start = System.currentTimeMillis();
        for (File file : new File(data).listFiles()) {
            ArrayList<LinkedList> fowardIndex = new ArrayList<LinkedList>();
            System.out.println("Now processing file: " + file);
            GZIPInputStream gzInputStream = new GZIPInputStream(new FileInputStream(file));
            DataInputStream inStream = new DataInputStream(gzInputStream);
            WarcRecord thisWarcRecord;
            while ((thisWarcRecord = WarcRecord.readNextWarcRecord(inStream)) != null) {
                if (thisWarcRecord.getHeaderRecordType().equals("response")) {
                    //processWARCRecord((String []) recordData.getLast());
                    LinkedList<Object> recordData = thisWarcRecord.getCleanRecord();
                    this.processWARCRecord((String[]) recordData.getLast(), (String) recordData.getFirst());
                    fowardIndex.add(recordData);
                    doc++;
                }
                if (doc % 1000 == 0) System.out.println(doc);
            }
            inStream.close();
            serialize(fowardIndex, fIndexPath + "/" + file.getName());


        }
        this.savePSMetadata();


    }

    public void processWARCRecord(String[] words, String title) {
        this.docsMap.putIfAbsent(title, this.stats[0]);
        HashMap<String, Integer> auxHash = new HashMap<>();
        for (String word : words) {
            if (auxHash.putIfAbsent(word, 1) == null) {
                if (!this.termsMap.containsKey(word)) {
                    this.termsMap.put(word, stats[2]);
                    this.freqTermDoc.put(stats[2], 1);
                    this.stats[2]++; //unique words
                } else {
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
        for (int k = 0; scanner.hasNext(); k++) {
            this.stats[k] = Integer.valueOf(scanner.next());
        }
    }

    public void dumpGInfo() throws FileNotFoundException {
        //this methods saves the global statistics about the whole dataset.

        try (FileWriter fw = new FileWriter("gstats.txt", true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {
            for (int k = 0; k < this.stats.length; k++) {
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
        try (FileWriter fw = new FileWriter("termsOccurence.csv", true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                String aux = pair.getKey() + "," + pair.getValue();
                out.println(aux);
                it.remove(); // avoids a ConcurrentModificationException
            }
        }
    }

    //EXTRA ************************************************************************************************************

    public void savePSMetadata() {
        serialize(this.termsMap, tPath);
        serialize(this.freqTermDoc, fPath);
        serialize(this.docsMap, docMapPath);
        serialize(this.stats, sPath);

    }


    public static void serialize(Object e, String file) {
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

    public void buildIndex() {
        /* To build the inverted index we retrive the collection of document that have been serialized in a list of
        * array strings. For each of the element of this list, which is a document, we call buffered index which process
        * its content and give add it to the buffered index (temporary has map that will be dumped in the array)*/

        doc = 0;
        start = System.currentTimeMillis();
        for (File file : new File(fIndexPath).listFiles()) {
            ArrayList<LinkedList<Object>> fowardIndex = (ArrayList<LinkedList<Object>>) deserialize(file.getAbsolutePath());
            LinkedList<Object> aux;
            Iterator it = fowardIndex.iterator();
            //start =  System.currentTimeMillis();
            while (it.hasNext()) {
                aux = (LinkedList) it.next();
                this.bufferedIndex((String[]) aux.getLast(), (String) aux.getFirst());
                doc++;
            }
            //this.dumpBuffer2(file.getName());
            //pointer = 0;
        }
    }

    public void bufferedIndex(String[] words, String title) {
        /* We use a temporaney HashMap to process the single documents present in the WRAC document and the we flush it
        and then we flush it in the buffer */


        HashMap<Integer, Integer> docFreq = new HashMap<>();    //auxFreq counts for each term how many times is used into the document
        HashSet<String> auxPair = new HashSet<>();
        for (int wIx = 0; wIx < words.length; wIx++) {
            if (wIx < words.length - this.distance) {
                for (int dIx = 1; dIx < this.distance; dIx++) {
                    String[] pair2Sort = {words[wIx], words[wIx + dIx]};
                    Arrays.sort(pair2Sort);
                    String pair = pair2Sort[0] + "-" + pair2Sort[1];
                    auxPair.add(pair);
                }
            }
            if (docFreq.putIfAbsent((getWId(words[wIx])), 1) != null) {
                docFreq.merge(getWId(words[wIx]), 1, Integer::sum);    //modified from this.freqTermDoc errore
            }
        }
        this.map2Buffer(docFreq, auxPair, words.length, getDId(title));
    }

    public void map2Buffer(HashMap<Integer, Integer> docFreq, HashSet<String> pairs, int docSize, int docId) {
        //We want to avoid huge hashmaps and for each warc document we flush it in a static data structure

        String pair;
        String[] words;
        Iterator it = pairs.iterator();
        while (it.hasNext()) {
            pair = it.next().toString();
            words = pair.split("-");
            int e1 = getWId(words[0]);
            int e2 = getWId(words[1]);
            //System.out.println(pair);
            this.buffer[pointer][0] = e1;
            this.buffer[pointer][1] = e2;
            this.buffer[pointer][2] = docId;
            this.buffer[pointer][3] = getBM25(e1, docSize, docFreq.get(e1));
            this.buffer[pointer][4] = getBM25(e2, docSize, docFreq.get(e2));
            if (pointer == buffer.length - 1) {
                naturalSelection();
                //pointer = (this.buffer.length/100)*20; we don't want to reuse it
                pointer = 0;
            } else pointer++;
            it.remove(); // avoids a ConcurrentModificationException
        }
    }

    public int getBM25(int id, int docLen, int f) {
        // global statistics for BM25
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

    public static double boundedKeep(int diff){
        /*we want to preserve the short lists and cut the long ones. The bounds are 100 and 1000. We have four cases
        * 20% > 1000  ->    keep 1000
        * 20% > 100   ->    keep 20%
        * diff > 100  ->    keep 100
        * diff < 100  ->    keep diff */

        return (diff*0.2 > 1000) ? 1000 : (diff*0.2 > 100) ? diff*0.2 : (diff > 100) ? 100 : diff;
    }


    public void naturalSelection() {
        //
        System.out.println("TIME TO CLEAN. Processed docs: " + doc);
        now = System.currentTimeMillis();
        this.sortBuffer();
        int [] nowPosting = this.buffer[0];
        int shifter = 0;
        int startPointer = 0;
        int keep;
        for (int k = 0; k < this.buffer.length; k++) {
            if (this.buffer[k][0] != nowPosting[0] | this.buffer[k][1] != nowPosting[1] | k == this.buffer.length - 1) {
                keep = (int) boundedKeep(k-startPointer);
                for (int k2 = startPointer; k2 < startPointer + keep; k2++) {
                    this.buffer[k2 - shifter][0] = this.buffer[k2][0];
                    this.buffer[k2 - shifter][1] = this.buffer[k2][1];
                    this.buffer[k2 - shifter][2] = this.buffer[k2][2];
                    this.buffer[k2 - shifter][3] = this.buffer[k2][3];
                    this.buffer[k2 - shifter][4] = this.buffer[k2][4];
                }
                //this.buffer[startPointer + keep - 1 - shifter][3] = 123456789;
                shifter += (k-startPointer) - keep; //we are going to overwrite the elements that we don't want to keep
                startPointer = k;
                nowPosting = this.buffer[startPointer];
            }

        }


        System.out.println("Sorting Time: " + (System.currentTimeMillis() - now) + "ms. Processing Time:" + 1000 / ((System.currentTimeMillis() - start) / doc) + "doc/sec");
        this.dumpBuffer3("upto" + doc, this.buffer.length-shifter);
    }

    public void sortBuffer() {
        Arrays.sort(this.buffer, new Comparator<int[]>() {
            @Override
            public int compare(int[] int1, int[] int2) {
                //if we have the same doc ids sort them based on the bm25
                if (int1[0] == int2[0]) {
                    if (int1[1] == int2[1]) {
                        return Integer.compare(int1[3] + int1[4], int2[3] + int2[4]);
                    } else return Integer.compare(int1[1], int2[1]);
                } else return Integer.compare(int1[0], int2[0]);
            }
        });
    }


    // STORE INVERTED INDEX ********************************************************************************************

    public void dumpBuffer(String name) {
        // Save the partial inverted index (a.k.a buffer) to disk

        String prefix;
        StringBuilder toFlush = new StringBuilder();
        for (int[] entry : this.buffer) {
            prefix = "";
            for (int num : entry) {
                toFlush.append(prefix + num);
                prefix = ",";
            }
            toFlush.append("\n");
        }
        File file = new File("dumps/" + name + ".txt");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            writer.append(toFlush);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void dumpBuffer2(String name) {
        // Save the partial inverted index (a.k.a buffer) to disk

        this.sortBuffer();
        StringBuilder toFlush = new StringBuilder();
        for (int[] entry : this.buffer) {
            toFlush.append(entry[0] + "," + entry[1] + "," + (entry[3] + entry[4]) + "," + entry[2] + "\n");
        }
        File file = new File("dumps/" + name + ".txt");

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            writer.append(toFlush);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void dumpBuffer3(String name, int limit) {
        // Save the partial inverted index (a.k.a buffer) to disk

        //this.sortBuffer();
        StringBuilder toFlush = new StringBuilder();
        int entry[];
        for (int k = 0; k < limit; k++) {
            entry = this.buffer[k];
            toFlush.append(entry[0] + "," + entry[1] + "," + (entry[3] + entry[4]) + "," + entry[2] + "\n");
        }
        System.out.println(limit);
        File file = new File(dPath + "/" + name + ".txt");

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            writer.append(toFlush);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}






 /*  public static void main(String [] args) throws IOException {
        String data = "/home/aalto/IdeaProjects/PredictiveIndex/data/test";
        InvertedIndex ps;
        if (Files.exists(Paths.get(tPath+ser))) {
            System.out.println("Deserializing Predictive Inverted...");
            ps = new InvertedIndex((HashMap<String, Integer>) deserialize(tPath+ser),  (HashMap<Integer, Integer>) deserialize(fPath+ser),
                    (HashMap<String, Integer>) deserialize(docMapPath+ser), (int[]) deserialize(sPath+ser));
            System.out.println("Predictive Index Deserialized");
        }else {
            ps = new InvertedIndex();
            ps.getCollectionMetadata(data);
        }
        ps.buildIndex();//
        //ImpactModel.buildImpactModel();
    }
}



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


    /*public ArrayList deserialize(File file){
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
    }*/