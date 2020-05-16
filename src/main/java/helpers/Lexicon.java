package helpers;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

import com.google.common.io.Files;
import javafx.util.Pair;
import org.apache.hadoop.mapred.InvalidFileTypeException;

public class Lexicon {
    private final File dataLexicon;
    private final Map<String, HashMap<String, List<String>>> lexicon = new HashMap<>();

    public Lexicon(String lexiconPath) throws IOException {
        this.dataLexicon = new File(lexiconPath);
        List<File> lexiconFiles = extractLexicons();
        for (File lexiconFile : lexiconFiles) {
            appendLexicon(lexiconFile);
        }
    }

    private List<File> extractLexicons() throws InvalidFileTypeException {
        List<File> lexiconFiles = new ArrayList<>();
        for (File file : Objects.requireNonNull(dataLexicon.listFiles())) {
            if (this.validLexiconFile(file)) {
                lexiconFiles.add(file);
            } else {
                throw new InvalidFileTypeException("Lexicon files must have the format <negative/positive>-words_<lang>");
            }
        }
        return lexiconFiles;
    }

    private boolean validLexiconFile(File file) {
        return file.isFile() && (file.getName().contains("positive") || file.getName().contains("negative") &&
                file.getName().split("_").length == 2);
    }

    @SuppressWarnings("UnstableApiUsage")
    private void appendLexicon(File dataLexicon) throws IOException {
        List<String> lines = Files.readLines(dataLexicon, Charset.defaultCharset());
        String lang = dataLexicon.getName().split("[_.]")[1];

        if (!lexicon.containsKey(lang)) { lexicon.put(lang, new HashMap<>()); }

        if (dataLexicon.getName().contains("positive")) {
            lexicon.get(lang).put("positive", lines);
        } else if (dataLexicon.getName().contains("negative")) {
            lexicon.get(lang).put("negative", lines);
        }
    }

    public Map<String, HashMap<String, List<String>>> getLexicon() {
        return lexicon;
    }

    public Pair<Long, Long> sentimentComputation(String lang, String message) {
        String wordsRegex = "[ ;:.,\\\\s]+";
        long positive = 0L;
        long negative = 0L;

        if (!lexicon.containsKey(lang)) { return new Pair<>(0L, -1L); } // Lexicon not available

        String[] splitMessage = message.split(wordsRegex);
        for (String word : splitMessage) {
            if (this.lexicon.get(lang).get("positive").contains(word)) {
                positive++;
            } else if (this.lexicon.get(lang).get("negative").contains(word)) {
                negative++;
            }
        }
        return new Pair<>((positive - negative), (long) splitMessage.length);
    }
}
