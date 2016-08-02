package PredictiveIndex;

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
        ///*
        String data = "/home/aalto/IdeaProjects/PredictiveIndex/data/en0000";
        InvertedIndex ps;
        if (Files.exists(Paths.get(tPath+ser))) {
            System.out.println("Deserializing Predictive Inverted...");
            ps = new InvertedIndex((HashMap<String, Integer>) deserialize(tPath+ser),  (HashMap<Integer, Integer>) deserialize(fPath+ser),
                    (HashMap<String, Integer>) deserialize(docMapPath+ser), (int[]) deserialize(sPath+ser));
            System.out.println("Predictive Index Deserialized");
        }else {
            ps = new InvertedIndex();
            ps.getCollectionMetadata2(data);
            //ps.readFromBinaryFile(fIndexPath + "/fowardIndex.dat");
        }
        ps.buildIndex();
        //*/

     /*InvertedIndex a = new InvertedIndex();
     int [][] aux = new int[][] {
                {3,4,3232,2,1},
                {1,5,3234,1,1},
                {3,6,7532, 4,1},
                {2,10, 53421, 5,1},
                {2,11,4141,1,1},
                {2,12,541,1,1},
                {1,2,41415,1,2},
                {3,1, 5145,1,2},
                {3,2,311,7,1},
                {2,9,41113,5,1},
                {1,3,2114,5,1},
                {3,4,3212,131,1},
                {1,5,412,4141,1},
                {3,6,4141,4141,1},
                {2,1,414,4141,1},
                {2,1,434,3,1},
                {2,12,22,3,1},
                {1,1,2,4,1, 34},
                {1,2,2131,4,1},
                {3,7,4,43,1},
                {3,8,4,4,1},
                {2,9,44,31,1},
                {1,3,31,31,1},
                {1,2,434,4343,1},
                {1,2,31,313,1},
                {3,7,31231,3231,1},
                {3,8,333,555,1},
                {2,9,5555,41,1},
                {2,44,41,4141,1}
        };
        a.buffer= aux;
        a.naturalSelection();*/

    }
}
