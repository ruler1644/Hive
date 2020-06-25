package my02_udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @Auther wu
 * @Date 2019/7/6  12:04
 */


//自定义函数，将一行数据中的单词切分，实现炸裂效果

//输入
//MapReduce,Hive,Spark,Flink

//输出
//field1
//MapReduce
//Hive
//Spark
//Flink
public class Code_01_UDTF extends GenericUDTF {

    List<String> dataList = new ArrayList<String>();

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs)
            throws UDFArgumentException {

        //定义输出数据的列名
        List<String> fieldNames = new ArrayList<String>();
        fieldNames.add("field1");

        //定义输出数据的类型
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    //每行数据调用一次process方法
    @Override
    public void process(Object[] args) throws HiveException {

        //获取数据
        String data = args[0].toString();

        //获取分割符
        String splitKey = args[1].toString();

        //切分数据
        String[] words = data.split(splitKey);

        //遍历数据
        for (String word : words) {

            //集合为复用的，首先清空集合，再将数据添加到集合
            dataList.clear();
            dataList.add(word);

            //写出数据
            forward(dataList);
        }
    }

    //关闭资源
    public void close() throws HiveException {

    }
}
