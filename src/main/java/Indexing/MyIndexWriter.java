package Indexing;

import Classes.Path;

import java.io.*;
import java.util.ArrayList;
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
    private int totalNumOfDocument = 0;

    /**
     * Map each term by its length
     */
    private HashMap<String, ArrayList<Integer>> allTermMap = new HashMap<>(200000);

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
            ArrayList<Integer> list = this.allTermMap.getOrDefault(s, new ArrayList<>());
            if (list.size() == 0) {
                this.allTermMap.put(s, list);
            }
            list.add(totalNumOfDocument);
        }
        this.totalNumOfDocument++;
    }

    /**
     * close the index writer, and you should output all the buffered content (if any).
     * if you write your index into several files, you need to fluse them here.
     */
    public void Close() throws IOException {
        if (this.totalNumOfDocument != this.docIdIndex.size()) throw new RuntimeException("Inbalance tree!");

        try (FileOutputStream fos = new FileOutputStream(this.indexPath + "terms")) {
            ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(fos));
            oos.writeObject(this.allTermMap);
            oos.close();
        }

        try (FileOutputStream fos = new FileOutputStream(this.indexPath + "docidx")) {
            ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(fos));
            oos.writeObject(new ArrayList<>(this.docIdIndex));
            oos.close();
        }

        this.allTermMap = null;
        this.docIdIndex = null;
        System.gc();
    }

}
