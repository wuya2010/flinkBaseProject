package com.yld.fwarehourse.etl;

import java.math.BigInteger;
import java.security.MessageDigest;

public class test {
    public static void main(String[] args) throws Exception {
        System.out.println(System.currentTimeMillis());
    }

    public static String generateHash(String input) {
        try {
            //参数校验
            if (null == input) {
                return null;
            }
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(input.getBytes());
            byte[] digest = md.digest();
            BigInteger bi = new BigInteger(1, digest);
            String hashText = bi.toString(16);
            while (hashText.length() < 32) {
                hashText = "0" + hashText;
            }
            return hashText;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
