/**
 * Created by aalto on 6/21/16.
 */
public class Test {

    public static void main(String[] args) throws Exception {
        System.out.println(System.getProperty("java.library.path"));

        /*String [] stopWordList =  {"a", "an", "and", "are", "as", "at", "be",
                "by","for", "from", "has", "he", "in", "is",
                "it", "its", "of", "on", "that", "the", "to",
                "was", "were", "will", "with"};

        String myIndex = "C:/Program Files/lemur/lemur4.12/src/app/obj/myIndex5";

        try {
            IndexEnvironment envI = new IndexEnvironment();
            envI.setStoreDocs(true);

            // create an Indri repository
            envI.setMemory(256000000);

            envI.setStemmer("krovetz");
            envI.setStopwords(stopWordList);

            envI.setIndexedFields( new String[] {"article", "header", "p", "title", "link"});
            envI.open(myIndex);
            envI.create( myIndex );

            // add xml files to the just created index i.e myIndex
            // xml_data is a folder which contains the list of xml files to be added

            File filesDir = new File("C:/NetbeanProg2/xml_data");
            File[] files = filesDir.listFiles();
            int noOffiles = files.length;
            for (int i = 0; i < noOffiles; i++) {
                System.out.println(files[i].getCanonicalPath() + "\t" + files[i].getCanonicalFile());
                envI.addFile(files[i].getCanonicalPath(), "xml");
            }
        } catch (Exception e) {
            System.out.println("issue is: " + e);
        }*/
    }
}
