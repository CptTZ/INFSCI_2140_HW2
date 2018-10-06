package Indexing;

import Classes.Config;
import Classes.Path;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.zip.GZIPOutputStream;

public class MyIndexWriter {

    /**
     * Prefix path for storing index data
     */
    private String indexPath;

    /**
     * Document No -> My Id indexer
     */
    private LinkedList<String> docIdIndex = new LinkedList<>();
    private int totalNumOfDocument = 0;

    private HashMap<Integer, HashMap<String, StringBuilder>> allTermMapByLength = new HashMap<>(100);

    /**
     * This constructor should initiate the FileWriter to output your index files
     * remember to close files if you finish writing the index
     */
    public MyIndexWriter(String type) throws IOException {
        if (type.equals("trecweb")) {
            this.indexPath = Path.IndexWebDir;
        } else if (type.equals("trectext")) {
            this.indexPath = Path.IndexTextDir;
        } else {
            throw new IOException("Type error");
        }
        File outputDir = new File(this.indexPath);
        if (!outputDir.exists() && !outputDir.mkdirs()) throw new IOException("mkdir failed");
    }

    /**
     * you are strongly suggested to build the index by installments
     * you need to assign the new non-negative integer docId to each document, which will be used in MyIndexReader
     */
    public void IndexADocument(String docno, String content) throws IOException {
        this.docIdIndex.add(docno);
        String[] contents = content.split(" ");
        for (String s : contents) {
            int c = s.length();
            HashMap<String, StringBuilder> termMap = this.allTermMapByLength.getOrDefault(c, new HashMap<>());
            if (termMap.size() == 0) {
                this.allTermMapByLength.put(c, termMap);
            }
            StringBuilder sb = termMap.getOrDefault(s, new StringBuilder());
            if (sb.length() == 0) {
                termMap.put(s, sb);
            }
            sb.append(totalNumOfDocument);
            sb.append(Config.TERM_POSTING_SPLITTER);
        }
        this.totalNumOfDocument++;
    }

    /**
     * close the index writer, and you should output all the buffered content (if any).
     * if you write your index into several files, you need to fluse them here.
     */
    public void Close() throws IOException {
        if (this.totalNumOfDocument != this.docIdIndex.size()) throw new RuntimeException("Inbalance tree!");

        String termTemplate = this.indexPath + Config.TERM_INDEX_NAME;

        this.allTermMapByLength.forEach((termHash, termMap) -> {
            try {
                BufferedWriter bw = new BufferedWriter(new FileWriter(String.format(termTemplate, termHash)));
                termMap.forEach((term, idStringBuilder) -> {
                    try {
                        bw.write(String.format(Config.TERM_MAPPER_FORMAT, term, idStringBuilder.toString()));
                        idStringBuilder.setLength(0);
                        idStringBuilder = null;
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
                bw.close();
                // Deep free memory
                termMap.clear();
                termMap = null;
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                System.gc();
            }
        });

        try (FileOutputStream fos = new FileOutputStream(this.indexPath + Config.DOC_INDEX_NAME)) {
            ObjectOutputStream oos = new ObjectOutputStream(new GZIPOutputStream(new BufferedOutputStream(fos)));
            oos.writeObject(new ArrayList<>(this.docIdIndex));
            oos.close();
        }

        this.allTermMapByLength.clear();
        this.allTermMapByLength = null;
        this.docIdIndex.clear();
        this.docIdIndex = null;
    }

}
