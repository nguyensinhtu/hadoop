/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Utils;

import java.util.GregorianCalendar;

/**
 *
 * @author tuns
 */
public class RanDomDOG {

    public static String rand(int startYear, int endYear) {
        GregorianCalendar gc = new GregorianCalendar();

        int year = getIntBetween(startYear, endYear);
        gc.set(gc.YEAR, year);

        int dayOfYear = getIntBetween(1, gc.getActualMaximum(gc.DAY_OF_YEAR));

        gc.set(gc.DAY_OF_YEAR, dayOfYear);
        String date_format = "%d/%d/%d";
        return String.format(date_format, gc.get(gc.MONTH) + 1, gc.get(gc.DATE), gc.get(gc.YEAR));
    }

    public static int getIntBetween(int s, int e) {
        return s + (int) Math.round(Math.random() * (e - s));
    }

    public static void main(String[] args) {
        System.out.println(rand(1990, 2010));
        System.out.println(rand(1990, 2010));
        System.out.println(rand(1990, 2010));
        System.out.println(rand(1990, 2010));
        System.out.println(getIntBetween(0, 1));
        System.out.println(getIntBetween(0, 1));

        System.out.println(getIntBetween(0, 1));
    }
}
