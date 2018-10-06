package Indexing;

import Classes.Path;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PreProcessedCorpusReader {

    private BufferedReader corpusReader;

    public PreProcessedCorpusReader(String type) throws IOException {
        if (type.equals("trecweb") || type.equals("trectext")) {
            this.corpusReader = new BufferedReader(new FileReader(Path.ResultHM1 + type));
        } else {
            throw new IOException("Type error");
        }
    }

    /**
     * read a line for docNo, put into the map with <"DOCNO", docNo>
     * read another line for the content , put into the map with <"CONTENT", content>
     */
    public Map<String, String> NextDocument() throws IOException {
        String docNo = this.corpusReader.readLine();
        if (docNo == null) {
            // File empty, should close
            this.corpusReader.close();
            return null;
        }
        Map<String, String> res = new HashMap<>(2,1f);
        res.put("DOCNO", docNo.trim());
        String content = this.corpusReader.readLine();
        res.put("CONTENT", content == null ? "" : content.trim());
        return res;
    }

}
