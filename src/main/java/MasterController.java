import java.io.*;
import java.util.*;

/**
 * +
 * The master controller process knows how many Reduce tasks there will be, say r such tasks.
 * The user typically tells the MapReduce system what r should be.
 * Then the master controller picks a hash function that takes a key as argument
 * and produces a bucket number from 0 to r-1.
 * Each key that is output by a Map task is hashed and its key-value pair is put in one of r local files.
 * Each file is destined for one of the r Reduce tasks.
 * <p>
 * The master controller merges the files from each Map task that are destined for a particular Reduce task and feeds the merged file to that process
 * as a sequence of key/list-of-values pairs.
 */
public class MasterController {

    void controller() throws IOException {
        String intermediateFileLocation = "files/intermediate.txt";

        MapReduce mapReduce = new MapReduce();

        File fileChunk1 = new File("files/input01.txt");
        mapReduce.map(fileChunk1, intermediateFileLocation);

        File fileChunk2 = new File("files/input02.txt");
        mapReduce.map(fileChunk2, intermediateFileLocation);

        // key-value pairs from each Map task are collected by master controller and sorted by key.
        // the values associated with each key are formed into a single list of values for that key.
        Map<String, List<Integer>> wordCountMap = getFileGroupedByKey(intermediateFileLocation);

        for (Map.Entry<String, List<Integer>> entry : wordCountMap.entrySet()) {
            // todo implement partitioning function: this ensures that all the same key values, end up being set to the same reduce task.
            mapReduce.reduce(entry.getKey(), entry.getValue(), "files/output.txt");
        }
    }

    public static Map<String, List<Integer>> getFileGroupedByKey(String fileName) throws FileNotFoundException {
        Map<String, List<Integer>> wordCountMap = new HashMap<>();
        Scanner scanner = new Scanner(new FileReader(fileName));
        while (scanner.hasNext()) {
            String[] wordCountArray = scanner.nextLine().split(", ");
            String key = wordCountArray[0]; // word
            int value = Integer.parseInt(wordCountArray[1]);
            if (wordCountMap.containsKey(key)) {
                wordCountMap.get(key).add(value);
            } else {
                ArrayList<Integer> listOfValues = new ArrayList<>();
                listOfValues.add(value);
                wordCountMap.put(key, listOfValues);
            }
        }
        return wordCountMap;
    }

}
