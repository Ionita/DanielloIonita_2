package preprocessing;
import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Alphabetizer {

    public static void main(String[] args) throws Exception {

        createFile("totalRecords.dat");
        appendFiles("totalRecords.dat", "/Users/mariusdragosionita/Documents/workspace/DanielloIonita_2/data/comments.dat");
        appendFiles("totalRecords.dat", "/Users/mariusdragosionita/Documents/workspace/DanielloIonita_2/data/friendships.dat");
        appendFiles("totalRecords.dat", "/Users/mariusdragosionita/Documents/workspace/DanielloIonita_2/data/posts.dat");

        order("totalRecords.dat");
    }

    public static void appendFiles(String permFile, String tmpFile) {

            // Appends file "tmpFile" to the end of "permFile"
            // Code adapted from:  https://www.daniweb.com/software-development/java/threads/44508/appending-two-java-text-files

        try {
            // create a writer for permFile
            BufferedWriter out = new BufferedWriter(new FileWriter(permFile, true));
            // create a reader for tmpFile
            BufferedReader in = new BufferedReader(new FileReader(tmpFile));
            String str;
            while ((str = in.readLine()) != null) {
                out.write(str);
                out.write(System.lineSeparator());
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

    private static void order (String inputFile) throws IOException {

        String outputFile = "query3_file.txt";

        FileReader fileReader = null;
        try {
            fileReader = new FileReader(inputFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        String inputLine;
        List<String> lineList = new ArrayList<String>();
        while ((inputLine = bufferedReader.readLine()) != null) {
            lineList.add(inputLine);
        }
        fileReader.close();

        Collections.sort(lineList);

        FileWriter fileWriter = new FileWriter(outputFile);
        PrintWriter out = new PrintWriter(fileWriter);
        for (String outputLine : lineList) {
            out.println(outputLine);
        }
        out.flush();
        out.close();
        fileWriter.close();

    }


}

