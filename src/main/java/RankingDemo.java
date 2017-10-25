

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Created  on 2017/8/3.
 */
public class RankingDemo extends Configured implements Tool {
    //自定义writer接口
    protected static class IntPair implements WritableComparable<IntPair>{
        private int first = 0;
        private int second = 0;

        public void set(int left,int right){
            first = left;
            second = right;
        }

        public int getFirst(){
            return first;
        }

        public int getSecond(){
            return second;
        }
        //反序列化，从流中的二进制转换成IntPair
        public void readFields(DataInput in) throws IOException {
            first = in.readInt();
            second = in.readInt();
        }
        //序列化，将IntPair转化成使用流传送的二进制
        public void write(DataOutput out) throws IOException {
            out.writeInt(first);
            out.writeInt(second);
        }
        @Override
        public int hashCode() {
            return first+"".hashCode() + second+"".hashCode();
        }
        @Override
        public boolean equals(Object right) {
            if (right instanceof IntPair) {
                IntPair r = (IntPair) right;
                return r.first == first && r.second == second;
            } else {
                return false;
            }
        }
        //对key排序时，调用的就是这个compareTo方法        //map做了个局部的排序
        public int compareTo(IntPair o) {
            if (first != o.first) {
                return first - o.first;
            } else if (second != o.second) {
                return second - o.second;
            } else {
                return 0;
            }
        }

    }

    /**
     * 在分组比较的时候，只比较原来的key，而不是组合key。
     * 假设我的GroupingComparator只去比较Key的前3个字符，那么
     baidu.A 1
     baidu.A 1
     baidu.B 1
     baidu.B 1
     就可以分到一个列表中，形成如下的输入
     {baidu.A ：[1,1,1,1]}
     并被一个Reducer一次性处理。
     */
    public static class GroupingComparator implements RawComparator<IntPair> {

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1, s1, Integer.SIZE/8, b2, s2, Integer.SIZE/8);
        }

        public int compare(IntPair o1, IntPair o2) {
            int first1 = o1.getFirst();
            int first2 = o2.getFirst();
            return first1 - first2;
        }
    }

    public static class MapClass extends Mapper<LongWritable, Text, IntPair, IntWritable> {

        private final IntPair key = new IntPair();
        private final IntWritable value = new IntWritable();

        @Override
        public void map(LongWritable inKey, Text inValue,Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(inValue.toString());
            int left = 0;
            int right = 0;
            if (itr.hasMoreTokens()) {
                left = Integer.parseInt(itr.nextToken());
                if (itr.hasMoreTokens()) {
                    right = Integer.parseInt(itr.nextToken());
                }
                key.set(left, right);
                value.set(right);
                context.write(key, value);
            }
        }
    }

    public static class ReduceClass extends Reducer<IntPair, IntWritable, Text, IntWritable> {
        private static final Text SEPARATOR = new Text("------------------------------------------------");
        private final Text first = new Text();

        @Override
        public void reduce(IntPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            context.write(SEPARATOR, null);
            first.set(Integer.toString(key.getFirst()));
            for(IntWritable value: values) {
                context.write(first, value);
            }
        }
    }


    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        Iterator iter=conf.iterator();
//        while (iter.hasNext()){
//            Map.Entry<Object, Object> item = (Map.Entry)iter.next();
//            System.out.println((String) item.getKey()+"   "+(String)item.getValue());
//        }
        conf.set("mapreduce.job.jar", "./target/RankingDemo-1.0-SNAPSHOT.jar");
        conf.set("mapreduce.app-submission.cross-platform", "true");
        //直接把jar包放到平台上运行所需要的配置
        conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName() );
//        conf.set("hadoop.http.staticuser.user", "scdx03" );

        Job job = Job.getInstance(conf,"secondary_sort");
//linux和windows的区别
        job.setJarByClass(RankingDemo.class);
//        Path job_output = new Path("/user/tony/secondarysort_out");
        Path job_output = new Path("/user/scdx03/secondarysort_out");
//        Path job_input = new Path("/user/tony/Secondarysort");
        Path job_input = new Path("/user/scdx03/Secondarysort/sc.txt");
        job.setMapOutputKeyClass(IntPair.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setGroupingComparatorClass(GroupingComparator.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);
//        job.setNumReduceTasks(1);输入文件的格式
        //TextInputFormat继承于FileInputFormat，FileInputFormat是按行切分的，每行作为一个Mapper 的输入
        //所以TextInputFormat是按行切分的
        job.setInputFormatClass(TextInputFormat.class);
        //输出文件的格式
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, job_input);
        //
        job_output.getFileSystem(conf).delete(job_output, true);
        FileOutputFormat.setOutputPath(job, job_output);
        //方法启动作业并检查进展情况
        job.waitForCompletion(true);
        return 0;
    }

//    public static void main(String[] args) throws Exception{
//        System.setProperty("hadoop.home.dir", "E:\\hadoop");
//        ToolRunner.run(new RankingDemo(),args);
//    }

    //    public class MapTest{
//        private Mapper Map;
//        private MapDriver driver;
//        @Before
//        public void init(){
//            Map=new MapClass();
//            driver=new MapDriver(Map);
//        }
//        @SuppressWarnings("unchecked")
//        @Test
//        public void testMap()throws Exception{
//            String text="hello world goodbye world hello hadoop goodbye hadoop";
//            driver.withInput(new LongWritable(), new Text(text))
//                    .withOutput(new Text("hello"),new IntWritable(1))
//                    .withOutput(new Text("world"),new IntWritable(1))
//                    .withOutput(new Text("goodbye"),new IntWritable(1))
//                    .withOutput(new Text("world"),new IntWritable(1))
//                    .withOutput(new Text("hello"),new IntWritable(1))
//                    .withOutput(new Text("hadoop"),new IntWritable(1))
//                    .withOutput(new Text("goodbye"),new IntWritable(1))
//                    .withOutput(new Text("hadoop"),new IntWritable(2)).runTest();
//        }
//
//    }
    public static void main(String[] args) throws Exception{
        System.setProperty("hadoop.home.dir", "E:\\hadoop");
        ToolRunner.run(new RankingDemo(),args);
    }
}