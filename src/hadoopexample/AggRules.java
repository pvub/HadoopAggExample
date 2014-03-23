/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package hadoopexample;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/* cd ~/Development/hadoop/hadoop-1.2.1 */
/* hadoop jar HadoopAggRulesExample.jar Dictionary Italian.txt output.txt */
/* cd output */
public class AggRules
{                
    public static void main(String[] args) throws Exception
    {
        ApplicationProperties prop = new ApplicationProperties();
        prop.load("app.properties");
        prop.print();
        Configuration conf = new Configuration();
        conf.set("AppProperties", "app.properties");
        Job job = new Job(conf, "aggrules");
        job.setJarByClass(AggRules.class);
        job.setMapperClass(QtyMapper.class);
        job.setReducerClass(AllQtyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapperEnvelope.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}