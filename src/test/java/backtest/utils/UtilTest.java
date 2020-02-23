package backtest.utils;

import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class UtilTest {
    @Test(timeout=100)
    public void TestTwoElems(){
        ArrayList<Double> arr = new ArrayList<>();
        arr.add(1d);
        arr.add(2d);
        assertEquals(0, Util.indexOfFirstEleNoLessThan(arr, 0.5));
        assertEquals(1, Util.indexOfFirstEleNoLessThan(arr, 1.5));
        assertEquals(0, Util.indexOfLastEleNoLargerThan(arr, 1.5));
        assertEquals(1, Util.indexOfLastEleNoLargerThan(arr, 2.5));
    }

    @Test(timeout=100)
    public void TestThreeElems(){
        ArrayList<Double> arr = new ArrayList<>();
        arr.add(1d);
        arr.add(2d);
        arr.add(3d);
        assertEquals(0, Util.indexOfFirstEleNoLessThan(arr, 1d));
        assertEquals(0, Util.indexOfLastEleNoLargerThan(arr, 1d));
        assertEquals(1, Util.indexOfFirstEleNoLessThan(arr, 2d));
        assertEquals(1, Util.indexOfLastEleNoLargerThan(arr, 2d));
        assertEquals(2, Util.indexOfFirstEleNoLessThan(arr, 3d));
        assertEquals(2, Util.indexOfLastEleNoLargerThan(arr, 3d));
    }
}
