package utils;

import java.util.ArrayList;
import java.util.List;

public class Regions {
    private static final List<String> regions = new ArrayList<>();

    static {
        regions.add("Berlin");
        regions.add("Hyderabad");
        regions.add("SF");
        regions.add("Dallas");
        regions.add("Delhi");
        regions.add("Calcutta");
        regions.add("New York");
        regions.add("Munich");
        regions.add("London");
        regions.add("Wales");
        regions.add("Hamburg");
        regions.add("Kiev");
    }

    public static List<String> getRegions() {
        return regions;
    }
}
