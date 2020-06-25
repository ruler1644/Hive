package my01_video;


/**
 * @Auther wu
 * @Date 2019/7/8  17:49
 */

//两个连续的字符作为分隔符，统计WordCount
public class Demo {
    public static void main(String[] args) {
        String str = "addeffffghhjjk";

        char[] arr1 = str.toCharArray();
        char[] arr2 = new char[2];
        char c1;
        char c2;
        for (int i = 0; i < arr1.length - 1; i++) {
            c1 = str.charAt(i);
            c2 = str.charAt(i + 1);
            if (c1 == c2) {
                arr2[0] = c1;
                arr2[1] = c2;
                String s = new String(arr2);
                String[] split = str.split(s);
                for (String s1 : split) {
                    System.out.print(s1 + "\t");
                }
                System.out.println();
            }
        }
    }
}
