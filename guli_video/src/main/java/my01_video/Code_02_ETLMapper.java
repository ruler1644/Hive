package my01_video;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Auther wu
 * @Date 2019/7/8  14:44
 */

//影音项目，数据清洗
public class Code_02_ETLMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    Text val = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        //数据获取
        String oriData = value.toString();

        //数据清洗
        String str = Code_01_ETLUtil.etlData(oriData);
        if (str == null) {
            return;
        }

        //数据写出
        val.set(str);
        context.write(NullWritable.get(), val);
    }
}
