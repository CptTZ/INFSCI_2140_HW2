package Classes;

public class Config {

    public static final int MAX_LOCAL_CACHE_ENTRIES = 9999;

    public static final String TERM_SPLITTER_REGEX = "\\|";
    public static final String TERM_MAPPER_FORMAT = "%s|%s%n";
    public static final char TERM_POSTING_SPLITTER = ',';

    public static final String DOC_INDEX_NAME = "doc.idx";
    public static final String TERM_INDEX_NAME = "term_%d.idx";

}
