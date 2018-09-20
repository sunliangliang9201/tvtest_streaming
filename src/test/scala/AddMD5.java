import java.math.BigInteger;
import java.security.MessageDigest;

public class AddMD5 {
    public String getMd51(String str) throws Exception{
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(str.getBytes());
        return new BigInteger(1, md.digest()).toString();
    }
    public String getMd52(String str) throws Exception{
        char[] hexDigits = {'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F','G','H','I','G','K','L','M','N'};
        byte[] btInput = str.getBytes();
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(btInput);
        byte[] btOutput = md.digest();
        int j = btOutput.length;
        char[] string = new char[j*2];
        int k = 0;
        for(int i = 0; i < j; i++){
            byte byte0 = btInput[i];
            string[k++] = hexDigits[byte0 >>> 4 & 0xf];
            string[k++] = hexDigits[byte0 & 0xf];
        }
        return new String(string);

    }
    public static void main(String[] args) throws Exception {
        AddMD5 md5 = new AddMD5();
        String str = "Km@:4?nLj4v(b(y+%h1";
        System.out.println(md5.getMd51(str));
        System.out.println(md5.getMd52(str));
    }
}
