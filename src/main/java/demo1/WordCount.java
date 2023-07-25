package demo1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class WordCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1. mapreduce处理的数据和输出的数据是在hdfs上的，所以要先连接hdfs
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop10:8020");

        // 2. 构建一个job，并告知job要启动什么代码（Mapper在哪里，Reducer在哪里）
        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCount.class);   // 新代码，这里需要改

        // 3. 设定文件读取的类和输出的类
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 4. 提供要读取的文件 和要输出的文件夹    // 新代码，这里需要改
        TextInputFormat.addInputPath(job,new Path("/demo1.txt"));
        TextOutputFormat.setOutputPath(job,new Path("/demo1_out"));  // 指定的文件夹不能存在

        // 5. 指定Mapper和Reducer具体的实现类  // 新代码，这里需要改
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 6. 设置map和reduce 输入输出的keyvalue 类型是什么
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 因为map输出的key-value类型一定与reduce输入的类型一致，所以我们只需要设定reduce输出的类型就可以了
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 7. 启动job
        boolean flag = job.waitForCompletion(true);
        if (flag) {
            System.out.println("程序运行成功！");
        }
    }
    static class WordCountMapper extends Mapper<LongWritable,Text, Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //拆开value，value为Text，转为String
            /*
            Text-->String      text.toString
            String-->Text      new Text(str)
             */
            //转为String
            String names = value.toString();
            //使用split进行分割
            String[] arr = names.split(" ");

            //对arr数组展开(因为这里我们一行有多个不同的元素)
            for (String name : arr) {
                context.write(new Text(name),new IntWritable(1));
            }
            /*
            map对数据处理后输出为：
            张三 1
            李四 1
             */
        }
    }

    static class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            /*
            张三 [1,1,1,1]
            李四 [1,1]
             */
            //累加器
            int sum=0;
            for (IntWritable value : values) {
                sum=sum+value.get();
            }
            //等sum累加完进行输出
            context.write(key,new IntWritable(sum));
        }
    }

}

