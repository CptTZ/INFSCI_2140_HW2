package Indexing;

import java.io.IOException;


public class MyIndexReader {

    /**
     * read the index files you generated in task 1
     * remember to close them when you finish using them
     * use appropriate structure to store your index
     */
    public MyIndexReader(String type) throws IOException {

    }

    /**
     * get the non-negative integer dociId for the requested docNo
     * If the requested docno does not exist in the index, return -1
     */
    public int GetDocid(String docno) {
        return -1;
    }

    /**
     * Retrieve the docno for the integer docid
     */
    public String GetDocno(int docid) {
        return null;
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
        return null;
    }

    /**
     * Return the number of documents that contains the token.
     */
    public int GetDocFreq(String token) throws IOException {
        return 0;
    }

    /**
     * Return the total number of times the token appears in the collection.
     */
    public long GetCollectionFreq(String token) throws IOException {
        return 0;
    }

    public void Close() throws IOException {
    }

}