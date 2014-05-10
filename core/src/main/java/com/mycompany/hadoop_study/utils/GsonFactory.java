package com.mycompany.hadoop_study.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * <p>
 * Factory class for All Gson Objects
 * </p>
 *
 */
public class GsonFactory {

    private static final GsonBuilder gsonBuilder = new GsonBuilder()
            .disableHtmlEscaping();

    private static final Gson gsonNonPretty;
    private static final Gson gsonPretty;

    static {
        gsonNonPretty = gsonBuilder.create();
        gsonPretty = gsonBuilder.setPrettyPrinting().create();
    }

    public static Gson getPrettyGson() { return gsonPretty; }
    public static Gson getNonPrettyGson() { return gsonNonPretty; }

    /**
     * Disable Cloning this object
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        throw new RuntimeException("Clone for class " + this.getClass() + " Disabled");
    }
}
