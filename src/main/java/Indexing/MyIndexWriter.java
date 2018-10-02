package Indexing;

import Classes.Path;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;

public class MyIndexWriter {

    /**
     * Prefix path for storing index data
     */
    private String indexPath;

    /**
     * Document No -> My Id indexer
     */
    private LinkedList<String> docIdIndex = new LinkedList<>();
    private long totalNumOfDocument = 0;

    /**
     * Map each term by its length
     */
    private HashMap<Integer, BufferedWriter> termWriterMap = new HashMap<>(50);

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
        if (!new File(this.indexPath).mkdirs()) throw new IOException("mkdir failed");
    }

    /**
     * you are strongly suggested to build the index by installments
     * you need to assign the new non-negative integer docId to each document, which will be used in MyIndexReader
     */
    public void IndexADocument(String docno, String content) throws IOException {
        this.docIdIndex.add(docno);
        this.totalNumOfDocument++;

        String[] contents = content.split(" ");
    }

    /**
     * close the index writer, and you should output all the buffered content (if any).
     * if you write your index into several files, you need to fluse them here.
     */
    public void Close() throws IOException {
        if (this.totalNumOfDocument != this.docIdIndex.size()) throw new RuntimeException("Inbalance tree!");

        this.termWriterMap.values().forEach(writer -> {
            if (writer == null) return;
            try {
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        try (BufferedWriter idxWriter = new BufferedWriter(new FileWriter(this.indexPath + "docidx"))) {
            int i = 0;
            for (String data : this.docIdIndex) {
                idxWriter.write(String.format("%s\t%d", data, i++));
            }
        }
    }

}
