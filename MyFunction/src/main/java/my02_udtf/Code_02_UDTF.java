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


//自定义函数，将一行数据炸裂为2列

//输入
//hello,world-at,guigu

//输出
//field1	field2
//hello	    world
//at	    guigu
public class Code_02_UDTF extends GenericUDTF {

    List<String> dataList = new ArrayList<String>();

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs)
            throws UDFArgumentException {

        //定义输出数据两列的列名
        List<String> fieldNames = new ArrayList<String>();
        fieldNames.add("field1");
        fieldNames.add("field2");

        //定义输出数据的类型
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {

        //获取数据
        String data = args[0].toString();

        //切分数据，hello,world-at,guigu
        String[] fields = data.split("-");
        String[] words1 = fields[0].split(",");
        String[] words2 = fields[1].split(",");

        //遍历数据，添加第一组数据
        dataList.clear();
        dataList.add(words1[0]);
        dataList.add(words1[1]);

        //写出数据
        forward(dataList);

        //遍历数据，添加第二组数据
        dataList.clear();
        dataList.add(words2[0]);
        dataList.add(words2[1]);

        //写出数据
        forward(dataList);
    }

    public void close() throws HiveException {

    }
}
