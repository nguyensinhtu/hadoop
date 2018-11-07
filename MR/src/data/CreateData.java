/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package data;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/**
 *
 * @author tuns
 */
public class CreateData {

    public static void bt4() throws IOException {
        File file = new File("/home/tuns/data/hadoop-data/patent.txt");
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));

        Random rand = new Random();
        Random rand2 = new Random();
        Random rand3 = new Random();
        int n = rand.nextInt(100) + 30;

        for (int i = 0; i < n; ++i) {
            int numofpatent = rand3.nextInt(40) + 3;
            for (int j = 0; j < numofpatent; ++j) {
                float r = i + 1 + rand2.nextFloat() * 1;
                writer.write(i + 1 + "\t" + r + "\n");
            }
        }

        writer.close();
    }

    public static void main(String[] args) throws IOException {
        bt4();
    }
}
