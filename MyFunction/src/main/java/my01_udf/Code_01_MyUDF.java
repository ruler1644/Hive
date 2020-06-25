package my01_udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @Auther wu
 * @Date 2019/7/6  9:53
 */

//方法名必须是evaluate，回调机制
public class Code_01_MyUDF extends UDF {

    public int evaluate(int a) {
        return a + 5;
    }

    public int evaluate(int a, int b) {
        return a + b + 5;
    }
}
