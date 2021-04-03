import java.io.*;
import java.util.*;

public class MapReduce {

    /**
     * Map tasks turns the chunk into a sequence of key-value pairs.
     * Map task can produce several key-value pairs with the same key.
     *
     * @param file    input file chunk.
     * @param context output file name of the map function.
     */
    public void map(File file, String context) throws IOException {
        // input file
        Scanner scanner = new Scanner(file);
        List<String> words = new ArrayList<>();
        while (scanner.hasNext()) {
            String[] wordsOnTheLine = scanner.nextLine().split("\\s+");
            words.addAll(Arrays.asList(wordsOnTheLine));
        }

        // write key value pairs to intermediate file.
        String mapTemporaryOutputFileLocation = "files/map-temporary-output.txt";
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(mapTemporaryOutputFileLocation));
        for (String word : words) {
            bufferedWriter.write(word + ", " + "1");
            bufferedWriter.newLine();
        }
        bufferedWriter.close();

        // call combiner
        for (Map.Entry<String, List<Integer>> entry : MasterController.getFileGroupedByKey(mapTemporaryOutputFileLocation).entrySet()) {
            combine(entry.getKey(), entry.getValue(), context);
        }
    }

    /**
     * @param key
     * @param values  list of values associated with the key.
     * @param context output file of the reduce function.
     */
    public void reduce(String key, List<Integer> values, String context) throws IOException {
        intSumReducer(key, values, context);
    }

    /**
     * An optional localized Reducer, can group data in the map phase.
     *
     * @param key
     * @param values  list of values associated with the key.
     * @param context output of the combine function.
     */
    public void combine(String key, List<Integer> values, String context) throws IOException {
        intSumReducer(key, values, context);
    }

    /**
     * @param key
     * @param values  list of values associated with the key.
     * @param context output file name.
     */
    private void intSumReducer(String key, List<Integer> values, String context) throws IOException {
        // output file
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(context, true));

        int sum = values.stream().mapToInt(Integer::intValue).sum();

        bufferedWriter.write(key + ", " + sum);
        bufferedWriter.newLine();

        bufferedWriter.close();
    }
}
