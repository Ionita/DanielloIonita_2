package preprocessing;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class OnlyPostCommented {


    public static void main(String[] args) throws Exception {

        createFile("onlyPCommented.txt");
        takeOnlySomeTuples("onlyPCommented.txt", "/home/simone/IdeaProjects/DanielloIonita_2/data/comments.dat");

    }

    public static void takeOnlySomeTuples(String permFile, String tmpFile) {

        // Appends file "tmpFile" to the end of "permFile"
        // Code adapted from:  https://www.daniweb.com/software-development/java/threads/44508/appending-two-java-text-files

        try {
            // create a writer for permFile
            BufferedWriter out = new BufferedWriter(new FileWriter(permFile, true));
            // create a reader for tmpFile
            BufferedReader in = new BufferedReader(new FileReader(tmpFile));
            String str;
            while ((str = in.readLine()) != null) {
                if(str.split("\\|")[5].equals("")) {
                    out.write(str);
                    out.write(System.lineSeparator());
                }
            }
            in.close();
            out.close();
        } catch (IOException e) {

        }
    }

    private static void createFile (String filename) {

        File f = new File(filename);
        try {
            f.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}