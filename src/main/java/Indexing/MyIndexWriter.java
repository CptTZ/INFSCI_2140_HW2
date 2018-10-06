package Indexing;

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

    private HashMap<Integer, HashMap<String, LinkedList<Integer>>> allTermMapByLength = new HashMap<>(100);

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
            HashMap<String, LinkedList<Integer>> termMap = this.allTermMapByLength.getOrDefault(c, new HashMap<>());
            if (termMap.size() == 0) {
                this.allTermMapByLength.put(c, termMap);
            }
            LinkedList<Integer> list = termMap.getOrDefault(s, new LinkedList<>());
            if (list.size() == 0) {
                termMap.put(s, list);
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

        String termTemplate = this.indexPath + "term_%d.idx";

        this.allTermMapByLength.forEach((termHash, termMap) -> {
            try {
                BufferedWriter bw = new BufferedWriter(new FileWriter(String.format(termTemplate, termHash)));
                termMap.forEach((term, idList) -> {
                    try {
                        StringBuilder sb = new StringBuilder();
                        for (Integer i : idList) {
                            sb.append(i);
                            sb.append(',');
                        }
                        // Free memory
                        idList.clear();
                        idList = null;
                        bw.write(String.format("%s:%s%n", term, sb.toString()));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
                bw.close();
                // Deep free memory
                termMap.clear();
                System.gc();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        try (FileOutputStream fos = new FileOutputStream(this.indexPath + "doc.idx")) {
            ObjectOutputStream oos = new ObjectOutputStream(new GZIPOutputStream(new BufferedOutputStream(fos)));
            oos.writeObject(new ArrayList<>(this.docIdIndex));
            oos.close();
        }

        this.docIdIndex.clear();
        this.docIdIndex = null;
    }

}
