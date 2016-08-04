package PredictiveIndex;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import PredictiveIndex.InvertedIndex.*;

import static PredictiveIndex.InvertedIndex.*;


/**
 * Created by aalto on 7/20/16.
 */
public class PredictiveIndex {

    public static void main(String [] args) throws IOException {
        /*We get the global statistics of the collection (fetch from memory if present, compute them if the opposite)
        * and than build the pair-distance from memory.*/

        String data = "/home/aalto/IdeaProjects/PredictiveIndex/data/en0000";
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
        ps.buildIndex();

    }
}
