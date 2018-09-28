package Indexing;

import Classes.Path;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class MyIndexWriter {

    private BufferedWriter indexWriter;

    /**
     * This constructor should initiate the FileWriter to output your index files
     * remember to close files if you finish writing the index
     */
    public MyIndexWriter(String type) throws IOException {
        String path;
        if (type.equals("trecweb")) {
            path = Path.IndexWebDir;
        } else if (type.equals("trectext")) {
            path = Path.IndexTextDir;
        } else {
            throw new IOException("Type error");
        }
        if (!new File(path).mkdirs()) throw new IOException("mkdir failed");
        this.indexWriter = new BufferedWriter(new FileWriter(path + "idx", false));
    }

    /**
     * you are strongly suggested to build the index by installments
     * you need to assign the new non-negative integer docId to each document, which will be used in MyIndexReader
     */
    public void IndexADocument(String docno, String content) throws IOException {

    }

    /**
     * close the index writer, and you should output all the buffered content (if any).
     * if you write your index into several files, you need to fluse them here.
     */
    public void Close() throws IOException {
        this.indexWriter.flush();
        this.indexWriter.close();
    }

}
