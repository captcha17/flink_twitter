package com.epam;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by Dzmitry_Rakushau on 6/19/2018.
 */
public class Util {
    public static Properties getTwitterProps() {
        Properties props = new Properties();
        InputStream input = null;

        try {

            String filename = "twitter.properties";
            input = Main.class.getClassLoader().getResourceAsStream(filename);
            if (input == null) {
                throw new RuntimeException("Unable to find " + filename);
            }

            //load a properties file from class path, inside static method
            props.load(input);

        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return props;
    }
}
