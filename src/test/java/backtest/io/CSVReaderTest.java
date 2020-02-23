package backtest.io;

import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class CSVReaderTest {
    @Test
    public void testLine2arr() throws Exception {
        CSVReader cr = new CSVReader();
        ArrayList<String> arr = cr.line2arr("a,b");
        assertEquals("a,b: size", 2, arr.size());
        assertEquals("a,b: a", "a", arr.get(0));
        assertEquals("a,b: b", "b", arr.get(1));
        arr = cr.line2arr("a,b,");
        assertEquals("a,b,: size", 3, arr.size());
        assertEquals("a,b,: a", "a", arr.get(0));
        assertEquals("a,b,: b", "b", arr.get(1));
        assertEquals("a,b,: ", "", arr.get(2));
        arr = cr.line2arr(",,");
        assertEquals(",,: size", 3, arr.size());
        assertEquals(",,: ", "", arr.get(0));
        assertEquals(",,: ", "", arr.get(1));
        assertEquals(",,: ", "", arr.get(2));
        arr = cr.line2arr("a,\"\"\"aaa\",\"aa\"\"\"\"\"");
        assertEquals("a,\"\"\"aaa\",\"aa\"\"\"\"\": size", 3, arr.size());
        assertEquals("a,\"\"\"aaa\",\"aa\"\"\"\"\": a", "a", arr.get(0));
        assertEquals("a,\"\"\"aaa\",\"aa\"\"\"\"\": \"aaa", "\"aaa", arr.get(1));
        assertEquals("a,\"\"\"aaa\",\"aa\"\"\"\"\": aa\"\"", "aa\"\"", arr.get(2));
        arr = cr.line2arr("a,\"a,\",b,\"c\",");
        assertEquals("a,\"a,\",b,\"c\",: size", 5, arr.size());
        assertEquals("a,\"a,\",b,\"c\",: a,", "a,", arr.get(1));
        assertEquals("a,\"a,\",b,\"c\",: ", "", arr.get(4));
        arr = cr.line2arr(",\"a\"\",\"\"\"");
        assertEquals(",\"a\"\",\"\"\": size", 2, arr.size());
        assertEquals(",\"a\"\",\"\"\": ", "", arr.get(0));
        assertEquals(",\"a\"\",\"\"\": a\",\"", "a\",\"", arr.get(1));
        arr = cr.line2arr("\"\"\"a\"\"a\",b");
        assertEquals("\"\"\"a\"\"a\",b: size", 2, arr.size());
        assertEquals("\"\"\"a\"\"a\",b: ", "\"a\"a", arr.get(0));
        arr = cr.line2arr("a,\"Fri, Jun. 6, 3:47 PM\"");
        assertEquals("a,\"Fri, Jun. 6, 3:47 PM\": size", 2, arr.size());
        assertEquals("a,\"Fri, Jun. 6, 3:47 PM\": ", "Fri, Jun. 6, 3:47 PM", arr.get(1));
    }
}