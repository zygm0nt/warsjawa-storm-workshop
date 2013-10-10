package pl.warsjawa.storm;

import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;

public class CountingBoltTest {

    @Test
    public void shouldCheckListFromMapIsSorted() {
        Map<String, AtomicInteger> map = prepareTestMap();
        List<Map.Entry<String, AtomicInteger>> result = new CountingBolt().orderMapByValue(map);

        assertTrue(isReverseSorted(toIntegersList(result)));
    }

    private List<Integer> toIntegersList(List<Map.Entry<String, AtomicInteger>> result) {
        List<Integer> integerList = new ArrayList<Integer>();
        for (Map.Entry<String, AtomicInteger> entry : result) {
            integerList.add(entry.getValue().get());
        }
        return integerList;
    }

    private Map<String, AtomicInteger> prepareTestMap() {
        Map<String, AtomicInteger> map = new HashMap<String, AtomicInteger>();
        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            map.put("key"+i, new AtomicInteger(random.nextInt(100)));
        }
        return map;
    }

    public static <T extends Comparable> boolean isReverseSorted(List<T> listOfT) {
        T previous = null;
        for (T t: listOfT) {
            if (previous != null && t.compareTo(previous) > 0) return false;
            previous = t;
        }
        return true;
    }
}
