package my01_video;

/**
 * @Auther wu
 * @Date 2019/7/8  14:43
 */

//清洗数据的工具类

/**
 * 过滤掉字段长度小于9的数据
 * 去掉类别字段中的空格
 * 相关视频ID字段的分隔符，由'\t'替换为'&'
 */
public class Code_01_ETLUtil {

    public static String etlData(String OriString) {

        StringBuffer sb = new StringBuffer();
        String[] fields = OriString.split("\t");

        //判断字段长度
        if (fields.length < 9) {
            return null;
        }

        //将第四个字段中的空格切掉，People & Blogs====>People&Blogs
        fields[3] = fields[3].replaceAll(" ", "");

        //将fields数组中的字段拼接
        for (int i = 0; i < fields.length; i++) {

            //处理前边9个字段，及最后一个字段：相关视频ID进行处理
            if (i < 9) {
                if (i == fields.length - 1) {
                    sb.append(fields[i]);
                } else {
                    sb.append(fields[i]).append("\t");
                }
            } else {
                if (i == fields.length - 1) {
                    sb.append(fields[i]);
                } else {
                    sb.append(fields[i]).append("&");
                }
            }
        }

        return sb.toString();
    }

//    public static void main(String[] args) {
//
//        System.out.println(etlData("RX24KLBhwMI\tlemonette\t697\tPeople & Blogs\t512\t24149\t4.22\t315\t474\tt60tW0WevkE\tWZgoejVDZlo\tXa_op4MhSkg\tMwynZ8qTwXA\tsfG2rtAkAcg\tj72VLPwzd_c\t24Qfs69Al3U\tEGWutOjVx4M\tKVkseZR5coU\tR6OaRcsfnY4\tdGM3k_4cNhE\tai-cSq6APLQ\t73M0y-iD9WE\t3uKOSjE79YA\t9BBu5N0iFBg\t7f9zwx52xgA\tncEV0tSC7xM\tH-J8Kbx9o68\ts8xf4QX1UvA\t2cKd9ERh5-8\n"));
//    }
}
