package com.adang.druid.proxy;

public class feature {
    public int height;
    public int totalChildren;

    public feature() {
        totalChildren = 0;
        height = 1;
    }

    public String toString() {
        return "feature{" +
                "height=" + height +
                ", totalChildren=" + totalChildren +
                '}';
    }

    public feature addTotalChildren(feature t) {
        totalChildren += t.totalChildren;
        return this; // for chaining
    }

    public feature addHeight(feature t) {
        height += t.height;
        return this; // for chaining
    }
}
