package Indexing;

import Classes.Config;
import Classes.Path;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.zip.GZIPInputStream;


public class MyIndexReader {

    private ArrayList<String> docIdIndex;
    private String[] emptyList = new String[0];
    private String termPathTemplate;

    /**
     * Local cache for token lookups
     */
    private HashMap<String, ArrayList<String>> tokenCache = new HashMap<>();

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
        this.termPathTemplate = basePath + Config.TERM_INDEX_NAME;
        readInAllData(basePath);
    }

    /**
     * Read in objects
     */
    private void readInAllData(String base) throws IOException {
        try (FileInputStream fis = new FileInputStream(base + Config.DOC_INDEX_NAME)) {
            ObjectInputStream ois = new ObjectInputStream(new GZIPInputStream(new BufferedInputStream(fis)));
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
        populateLocalCache(token);

        List<String> allDocs = this.tokenCache.get(token);
        HashSet<String> docsSet = new HashSet<>(allDocs);

        int docidLen = docsSet.size();
        int[][] res = new int[docidLen][2];
        int i = 0;
        for (String docid : docsSet) {
            res[i][0] = Integer.parseInt(docid);
            res[i][1] = (int) allDocs.parallelStream().filter(id -> id.equals(docid)).count();
            i++;
        }
        return res;
    }

    /**
     * Add data to local cache for faster access
     */
    private void populateLocalCache(String token) throws IOException {
        if (!this.tokenCache.containsKey(token)) {
            // Flush cache if it's too large
            if (this.tokenCache.size() > Config.MAX_LOCAL_CACHE_ENTRIES) clearAllTokenCache();
            this.tokenCache.put(token, findOneTermPostings(token));
        }
    }

    /**
     * Read data from one term
     */
    private ArrayList<String> findOneTermPostings(String term) throws IOException {
        int termLen = term.length();
        ArrayList<String> res = new ArrayList<>(0);
        // Read Gzipped index file
        GZIPInputStream gzIn = new GZIPInputStream(new FileInputStream(new File(String.format(this.termPathTemplate, termLen))));
        BufferedReader br = new BufferedReader(new InputStreamReader(gzIn, Charset.defaultCharset()));

        Optional<String> termPos = br.lines().parallel()
                .filter(s -> {
                    String[] data = s.split(Config.TERM_SPLITTER_REGEX);
                    if (data.length != 2) return false;
                    return data[0].equals(term);
                }).findFirst();

        if (termPos.isPresent()) {
            String[] pos = termPos.get().split(Config.TERM_SPLITTER_REGEX)[1].split(String.valueOf(Config.TERM_POSTING_SPLITTER));
            res = new ArrayList<>(pos.length);
            for (String p : pos) {
                String pT = p.trim();
                if (pT.isEmpty()) continue;
                res.add(pT);
            }
        }
        br.close();
        return res;
    }

    /**
     * Return the number of documents that contains the token.
     */
    public int GetDocFreq(String token) throws IOException {
        populateLocalCache(token);
        List<String> res = this.tokenCache.get(token);
        return new HashSet<>(res).size();
    }

    /**
     * Return the total number of times the token appears in the collection.
     */
    public long GetCollectionFreq(String token) throws IOException {
        populateLocalCache(token);
        return this.tokenCache.get(token).size();
    }

    private void clearAllTokenCache() {
        this.tokenCache.forEach((k, v) -> {
            v.clear();
            v = null;
        });
        this.tokenCache.clear();
        System.gc();
    }

    public void Close() throws IOException {
        clearAllTokenCache();
        this.tokenCache = null;
        this.docIdIndex.clear();
        this.docIdIndex = null;
        System.gc();
    }

}
