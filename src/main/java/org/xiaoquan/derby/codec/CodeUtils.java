package org.xiaoquan.derby.codec;

import org.xiaoquan.derby.Constants;

import java.io.DataOutputStream;
import java.io.UnsupportedEncodingException;

/**
 * Created by XiaoQuan on 2015/1/7.
 */
public class CodeUtils {

    public static byte[] stringToBytes(String value) throws UnsupportedEncodingException {
        return value.getBytes(Constants.CHARACTER_SET);
    }

    public static void writeString(DataOutputStream dataOutputStream, String string) throws Exception {
        byte[] stringBytes = stringToBytes(string);
        dataOutputStream.writeShort((short) stringBytes.length);
        dataOutputStream.write(stringBytes);
    }
}
