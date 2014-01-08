/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package hadoopexample;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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
    public static class QtyMapper extends Mapper<Text, Text, Text, IntWritable>
    {                        
        private LineKeyList createKeys()
        {
            LineKeyList keys = new LineKeyList();

            ArrayList<Integer> indices = new ArrayList<Integer>();
            indices.add(6); // TOTAL
            indices.add(9); // NATIONAL
            LineKey rowKey1 = new LineKey(new ArrayList<Integer>(indices));
            keys.AddKey(rowKey1);

            indices.clear();
            indices.add(5); // PLID
            indices.add(8); // RETAILER
            LineKey rowKey2 = new LineKey(new ArrayList<Integer>(indices));
            keys.AddKey(rowKey2);

            return keys;
        }

        private LineValueList createValues()
        {
            LineValueList values = new LineValueList();

            LineValue value = new LineValue<Integer>(10, "int", "sum");
            values.AddLineValue(value);

            return values;
        }
        
        private Integer getInt(String strValue)
        {
            Integer i = new Integer(0);
            try
            {
                i = Integer.parseInt(strValue);
                System.out.println("#@# mapper rowQty:" + i);
            }
            catch (NumberFormatException e) {
                ;
            }
            return i;
        }
        
        private Double getDouble(String strValue)
        {
            Double d = new Double(0.0);
            try
            {
                d = Double.parseDouble(strValue);
                System.out.println("#@# mapper rowQty:" + d);
            }
            catch (NumberFormatException e) {
                ;
            }
            return d;
        }

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException
        {
            LineKeyList rowKeyList = createKeys();
            LineValueList rowValueList = createValues();
            
            IntWritable qty = new IntWritable();
            StringTokenizer itr = new StringTokenizer(key.toString(),"|");
            int iIndex = 0;
            LineKey k = null;
            LineValue v = null;
            int rowQty = 0;
            System.out.println("#@# mapper tokens:" + itr.countTokens() + " line:" + key);
            while (itr.hasMoreTokens())
            {
                k = rowKeyList.GetMatchingKey(iIndex);
                v = rowValueList.getMatchingLineValue(iIndex);
                
                if (k != null) 
                {
                    String strFieldValue = itr.nextToken("|");
                    k.AddKeyField(strFieldValue);
                }
                else if (iIndex == 10)
                {
                    String strQty = itr.nextToken("|");
                    System.out.println("#@# mapper strQty:" + strQty);
                    try
                    {
                        rowQty = Integer.parseInt(strQty);
                        System.out.println("#@# mapper rowQty:" + rowQty);
                    }
                    catch (NumberFormatException e) {
                        ;
                    }
                    
                }
                else
                {
                    itr.nextToken("|");
                }
                ++iIndex;
            }
            
            qty = new IntWritable(rowQty);
            
            ArrayList<LineKey> keys = rowKeyList.GetKeys();
            for (LineKey lk : keys)
            {
                key = new Text(lk.ToString());
                System.out.println("#@# mapper out key:" + key + " qty:" + qty);
                context.write(key, qty);
            }
        }
    }
    
    public static class AllQtyReducer
    extends Reducer<Text,IntWritable,Text,IntWritable>
    {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values,
        Context context
        ) throws IOException, InterruptedException
        {
            Integer totalQty = new Integer(0);
            for (IntWritable val : values)
            {
                totalQty += val.get();
            }
            result.set(totalQty);
            System.out.println("#@# reducer key:" + key + " result:" + result);
            context.write(key, result);
        }
    }
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "aggrules");
        job.setJarByClass(AggRules.class);
        job.setMapperClass(QtyMapper.class);
        job.setReducerClass(AllQtyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}