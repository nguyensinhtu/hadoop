/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author tuns
 */
public class CSV implements Serializable {

    static final private int NUMMARK = 10;
    static final private char COMMA = ',';
    static final private char DQUOTE = '"';

    static final Pattern pattern = Pattern.compile(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

    public static String[] split(String s)
            throws java.io.IOException {
//        Matcher matcher = pattern.matcher(s);
//
//        List<String> fields = new ArrayList<>();
//        while (matcher.find()) {
//            fields.add(matcher.group());
//        }
        String []str = pattern.split(s);
        return str;
    }

    public static void main(String[] args) throws Exception {
        File file = new File("/home/tuns/data/hadoop-data/part-m-00000");

        BufferedReader reader = new BufferedReader(new FileReader(file));

        String line = null;
        reader.readLine();
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
            String []str = CSV.split(line);
            System.out.println(str[2]);
            System.out.println(str[5]);
        }
    }

}
