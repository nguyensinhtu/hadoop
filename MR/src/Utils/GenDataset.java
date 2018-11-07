/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/**
 *
 * @author tuns
 */
public class GenDataset {

    public static void GenHealthCareData(int numOfRecord, String path) throws IOException{
        // init value for each fields
        int idPatient = 0;
        String idPatient_format = "%04d";
        int lenOfName = 4;
        int startYear = 1990;
        int endYear = 2001;
        int lenOfNumber = 9;
        String Email_format = "%s@xxx.com";
        int lenOfSSN = 0;
        String[] gender = {"M", "F"};
        String[] Disease = {"Diabetes", "PCOS", "Fever", "Cold", "Blood Pressure"
            + "Malaria"
        };

        int minWeight = 30;
        int maxWeight = 100;

        Random rand = new Random(123);
        Random rand1 = new Random(567);
        RandomString randomString = new RandomString(lenOfName, rand, RandomString.lower + RandomString.upper);
        RandomString randomNumberStr = new RandomString(lenOfNumber, rand1, RandomString.digits);
        /////////////
        String line_format = "%s,%s,%s,%s,%s,%s,%s,%s,%s\n";
        String[] headers = {"PatientID", "Name", "DOB", "Phone Number", "Email andress",
            "SSN", "Gender", "Disese", "Weight"
        };

        File file = new File(path);
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(file));
            writer.write(String.format(line_format, headers));
            for (int i = 0; i < numOfRecord; ++i) {
                String nameStr = randomString.nextString();
                String line = String.format(line_format, String.format(idPatient_format, idPatient++),
                         nameStr, RanDomDOG.rand(startYear, endYear),
                        randomNumberStr.nextString(), String.format(Email_format, nameStr),
                        randomNumberStr.nextString(), gender[RanDomDOG.getIntBetween(0, 1)],
                        Disease[RanDomDOG.getIntBetween(0, Disease.length - 1)],
                        Integer.toString(RanDomDOG.getIntBetween(minWeight, maxWeight)));
                writer.write(line);
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }finally{
            writer.close();
        }
    }

    public static void main(String[] args) throws IOException{
//        System.out.println(String.format("%03d", 12));
        GenHealthCareData(100, "/home/tuns/data/hadoop-data/healthcare.csv");
    }
}
