package my01_video;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Auther wu
 * @Date 2019/7/8  14:44
 */
//my01_video.Code_03_ETLRunner
//上传到hadoop集群，以hadoop jar的方式运行
public class Code_03_ETLRunner implements Tool {

    private Configuration configuration;

    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(configuration);
        job.setJarByClass(Code_03_ETLRunner.class);
        job.setMapperClass(Code_02_ETLMapper.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        //不需要Reduce
        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }

    @Override
    public void setConf(Configuration conf) {
        configuration = conf;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    //可以解析hadoop参数
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        try {
            int result = ToolRunner.run(conf, new Code_03_ETLRunner(), args);
            if (result == 0) {
                System.out.println("success");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
