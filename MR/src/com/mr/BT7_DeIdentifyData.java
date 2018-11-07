package com.mr;

import Utils.CSV;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author tuns
 */
public class BT7_DeIdentifyData {

    // xác định những trường nào được encrypt
    public static int[] EncrypCol = {1, 2, 3, 4, 5, 7};
    private static String Seckey = "tuns1997";
    
    // lua chon mode(encrypt_mode vs decrypt_mode) de chay map reduce task 
    public static int MODE = Cipher.DECRYPT_MODE;

    /**
     * param key : key dùng cho quá trình encrypt và decryption, data : block
     * byte data cần encrypt or dencrypt MODE : Cipher.MODE(encrypt or decrypt)
     * hàm encrypte date sử dụng thuật toán AES Output : encode string của data
     * đã encrypt
     */
    public static String encryptOrDeCrypt(String key, byte[] data) {

        String final_result = "";
        byte[] real_data = null;
        if (MODE == Cipher.DECRYPT_MODE) {
            real_data = Base64.getDecoder().decode(data);
        } else {
            real_data = data;
        }

        try {
            // thuật toán AES
            // encrypt mode : CBC
            // padding scheme : PKCS5Padding 
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");

            // tao key
            SecretKeySpec SecKey = SymetricKey(key, 16, "AES");
            //init cipher
            cipher.init(MODE, SecKey, new IvParameterSpec(new byte[16]));

            // encrypt or decrypt data
            byte[] byte_tmp = cipher.doFinal(real_data);

            // encode encrypted_data sang string de luu xuong
            // su dung base64 encode or decode
            if (MODE == Cipher.ENCRYPT_MODE) {
                final_result = Base64.getEncoder().encodeToString(byte_tmp);
            } else {
                final_result = new String(byte_tmp);
            }
        } catch (BadPaddingException | IllegalBlockSizeException | InvalidKeyException
                | NoSuchAlgorithmException | NoSuchPaddingException | InvalidAlgorithmParameterException e) {
            System.out.println("encryptOrDeCrypt : " + e.getMessage());
        }
        return final_result;
    }

    // generate valid key : 16, 32 bytes for AES algorithm
    public static SecretKeySpec SymetricKey(String strKey, int len, String algo) {
        byte[] key = new byte[len];
        if (strKey.length() < len) {
            int missLen = len - strKey.length();
            for (int i = 0; i < missLen; ++i) {
                strKey += " ";
            }
        }
        try {
            key = strKey.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            System.out.println(e.getMessage());
        }

        SecretKeySpec secKey = new SecretKeySpec(key, algo);
        return secKey;
    }

    // map phrase
    public static class Map extends Mapper<LongWritable, Text, NullWritable, Text> {

        @Override
        public void map(LongWritable key, Text value, Context output) {

            String line = value.toString();
            String[] str = null;
            try {
                // tranh header file
                if (key.get() == 0) {
                    output.write(NullWritable.get(), value);
                    return;
                }else
                str = CSV.split(line);

                for (int col : EncrypCol) {
                    str[col] = encryptOrDeCrypt(Seckey, str[col].getBytes());
                }

                String line_format = "%s,%s,%s,%s,%s,%s,%s,%s,%s";

                String result = String.format(line_format, (String[]) str);
                output.write(NullWritable.get(), new Text(result));
            } catch (IOException | InterruptedException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    /**
     * ham main
     */
//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//
//        Job job = Job.getInstance(conf, "DeIdentifyData");
//
//        // set class mà hadoop sẽ xác định file .jar nào được send tới các nodes để thực hiện
//        // map task và reduce task (mapp class, reduce task phải là một phần của class này)
//        job.setJarByClass(BT7_DeIdentifyData.class);
//
//        // ở đây chúng ta chỉ dùng reduce task
//        job.setNumReduceTasks(0);
//        job.setMapperClass(Map.class);
//
//        job.setInputFormatClass(TextInputFormat.class);
//
//        job.setOutputKeyClass(NullWritable.class);
//        job.setOutputValueClass(Text.class);
//
//        FileInputFormat.addInputPath(job, new Path(args[1]));
//        FileOutputFormat.setOutputPath(job, new Path(args[2]));
//
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//
//    }
}
