package Indexing;

import Classes.Path;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;


public class MyIndexReader {

    private ArrayList<String> docIdIndex;
    private HashMap<String, ArrayList<Integer>> allTermMap;
    private ArrayList<Integer> emptyList = new ArrayList<>(0);

    /**
     * read the index files you generated in task 1
     * remember to close them when you finish using them
     * use appropriate structure to store your index
     */
    public MyIndexReader(String type) throws IOException {
        String basePath;
        if (type.equals("trecweb")) {
            basePath = Path.IndexWebDir;
        } else if (type.equals("trectext")) {
            basePath = Path.IndexTextDir;
        } else {
            throw new IOException("Type error");
        }
        readInAllData(basePath);
    }

    /**
     * Read in objects
     */
    private void readInAllData(String base) throws IOException {
        try (FileInputStream fis = new FileInputStream(base + "terms")) {
            ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(fis));
            try {
                this.allTermMap = (HashMap<String, ArrayList<Integer>>) ois.readObject();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            ois.close();
        }

        try (FileInputStream fis = new FileInputStream(base + "docidx")) {
            ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(fis));
            try {
                this.docIdIndex = (ArrayList<String>) ois.readObject();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            ois.close();
        }
        System.gc();
    }

    /**
     * get the non-negative integer dociId for the requested docNo
     * If the requested docno does not exist in the index, return -1
     */
    public int GetDocid(String docno) {
        return this.docIdIndex.indexOf(docno);
    }

    /**
     * Retrieve the docno for the integer docid
     */
    public String GetDocno(int docid) {
        return this.docIdIndex.get(docid);
    }

    /**
     * Get the posting list for the requested token.
     * <p>
     * The posting list records the documents' docids the token appears and corresponding frequencies of the term, such as:
     * <p>
     * [docid]		[freq]
     * 1			3
     * 5			7
     * 9			1
     * 13			9
     * <p>
     * ...
     * <p>
     * In the returned 2-dimension array, the first dimension is for each document, and the second dimension records the docid and frequency.
     * <p>
     * For example:
     * array[0][0] records the docid of the first document the token appears.
     * array[0][1] records the frequency of the token in the documents with docid = array[0][0]
     * ...
     * <p>
     * NOTE that the returned posting list array should be ranked by docid from the smallest to the largest.
     */
    public int[][] GetPostingList(String token) throws IOException {
        ArrayList<Integer> allDocs = this.allTermMap.getOrDefault(token, emptyList);
        HashSet<Integer> docsSet = new HashSet<>(allDocs);

        int docidLen = docsSet.size();
        int[][] res = new int[docidLen][2];
        int i = 0;
        for (Integer docid : docsSet) {
            res[i][0] = docid;
            res[i][1] = (int) allDocs.parallelStream().filter(id -> id.equals(docid)).count();
            i++;
        }
        return res;
    }

    /**
     * Return the number of documents that contains the token.
     */
    public int GetDocFreq(String token) throws IOException {
        ArrayList<Integer> res = this.allTermMap.getOrDefault(token, emptyList);
        return new HashSet<>(res).size();
    }

    /**
     * Return the total number of times the token appears in the collection.
     */
    public long GetCollectionFreq(String token) throws IOException {
        return this.allTermMap.getOrDefault(token, emptyList).size();
    }

    public void Close() throws IOException {
        this.allTermMap.clear();
        this.docIdIndex.clear();
        System.gc();
    }

}