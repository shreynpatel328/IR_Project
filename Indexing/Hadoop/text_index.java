//package org.myorg;

import java.io.IOException;
import java.util.*;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import java.net.URI;
import java.lang.String;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

public class text_index
{
    public static class Map extends Mapper<LongWritable, Text, Text, Text> //MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
    {

       // private HashMap<String, String> StateMap = new HashMap<String, String>();
        private BufferedReader brReader;
        private String strStateName = "";
        private Text txtMapOutputKey = new Text("");
        private Text txtMapOutputValue = new Text(""); 
		
		
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
           // brReader = new BufferedReader(new FileReader(eachPath.toString()));
           // strLineRead = brReader.readLine();
			try
            {
                if(value != null)
                {
                    String line=value.toString();  
					//System.out.println(line);
					int docid;
					int line_no;
					String text;
					int shift=0;
					int c=0;
					String text_array[] =new String[60];
					String[] array = {"a","about","above","after","again","against","all","am","an","and", "any","are","aren't","as","at","be","because","been","before","being",
									  "below","between","both","but","by","can't","cannot","could","couldn't","did","didn't","do","does","doesn't","doing","don't","down","during",
									  "each","few","for","from","further","had","hadn't","has","hasn't","have","haven't","having","he","he'd","he'll","he's","her","here","here's",
									  "hers","herself","him","himself","his","how","how's","i","i'd","i'll","i'm","i've","if","in","into","is","isn't","it","it's","its","itself",
									  "let's","me","more","most","mustn't","my","myself","no","nor","not","of","off","on","once","only","or","other","ought","our","ours","ourselves",
									  "out","over","own","same","shan't","she","she'd","she'll","she's","should","shouldn't","so","some","such","than","that","that's","the","their",
									  "theirs","them","themselves","then","there","there's","these","they","they'd","they'll","they've","they're","this","those","through","to","too",
									  "under","until","up","very","was","wasn't","we","we'd","we'll","we're","we've","were","weren't","what","what's","when","when's","where","where's",
									  "which","while","whos's","who","whom","why","why's","with","won't","would","wouldn't","you","you'd","you'll","you're","you've","your","yours",
									  "yourself","yourselves","RT","1","2","3","4","5","6","7","8","9","0","$","$$","$$$","%"," ","))",")))","))))","++","+++","+","|||","%%","(","(("};
					
					if(line != null && !line.isEmpty())
                    {
                        Arrays.fill(text_array, null);
						shift=0;
						String[] parts= line.split("&!@");
						docid = Integer.parseInt(parts[0]);
						line_no=Integer.parseInt(parts[1]);
						text = parts[4];
						
						String partition[]=text.split(" +");
						for(int i=0;i<partition.length;i++)
						{	
							String part=partition[i];
							c=0;
							if(partition[i].contains("@")==false && partition[i].contains("~")==false && partition[i].contains("_")==false && partition[i].contains("#")==false && partition[i].contains(".")==false && partition[i].contains("\\")==false && partition[i].contains("&")==false)
							{
								if(partition[i].contains("!")==true && part.length()>0)
								{
									int index=partition[i].indexOf("!");
									partition[i]=partition[i].substring(0,index);
								}

								if(partition[i].contains("$")==true && part.length()>0)
								{
									int index=partition[i].indexOf("$");
									if(index==0)
									partition[i]=partition[i].substring(1,partition[i].length());
								}
								if(partition[i].contains("%")==true && part.length()>0)
								{
									int index=partition[i].indexOf("%");
									if(index==0)
									partition[i]=partition[i].substring(1,partition[i].length());
								}
								if(partition[i].contains("+")==true && part.length()>0)
								{
									int index=partition[i].indexOf("+");
									if(index==0)
									partition[i]=partition[i].substring(1,partition[i].length());
								}
								if(partition[i].contains("''")==true && part.length()>0)
								{
									int index=partition[i].indexOf("''");
									if(index==0)
									partition[i]=partition[i].substring(2,partition[i].length());
								}
								
								if(partition[i].contains("'")==true && part.length()>0)
								{
									int index=partition[i].indexOf("'");
									if(index==0)
									partition[i]=partition[i].substring(1,partition[i].length());
								}
								if(partition[i].contains("'")==true && part.length()>0)
								{
									int index=partition[i].indexOf("'");
									if(index==0)
									partition[i]=partition[i].substring(1,partition[i].length());
								}
								
								if(partition[i].contains("'")==true && part.length()>0)
								{
									int index=partition[i].lastIndexOf("'");
									if(index==partition[i].length()-1)
									partition[i]=partition[i].substring(0,partition[i].length()-1);
								}
								
								if(partition[i].contains("*")==true && part.length()>2)
								{
									int index=partition[i].indexOf("*");
									if(index==0)
									partition[i]=partition[i].substring(1,part.length());
								}
								
								if(partition[i].contains("*")==true && part.length()>0)
								{
									int index=partition[i].lastIndexOf("*");
									if(index==partition[i].length()-1)
									partition[i]=partition[i].substring(0,partition[i].length()-1);
								}
								if(partition[i].contains("%")==true && part.length()>0)
								{
									int index=partition[i].lastIndexOf("%");
									if(index==partition[i].length()-1)
									partition[i]=partition[i].substring(0,partition[i].length()-1);
								}
								if(partition[i].contains(")")==true && part.length()>0)
								{
									int index=partition[i].lastIndexOf(")");
									if(index==partition[i].length()-1)
									partition[i]=partition[i].substring(0,partition[i].length()-1);
								}
								if(partition[i].contains("+")==true && part.length()>0)
								{
									int index=partition[i].lastIndexOf("+");
									if(index==partition[i].length()-1)
									partition[i]=partition[i].substring(0,partition[i].length()-1);
								}
								if(partition[i].contains("?")==true && part.length()>0)
								{
									int index=partition[i].lastIndexOf("?");
									if(index==partition[i].length()-1)
									partition[i]=partition[i].substring(0,partition[i].length()-1);
								}
								if(partition[i].contains("(")==true && part.length()>0)
								{
									int index=partition[i].indexOf("(");
									if(index==0)
									partition[i]=partition[i].substring(1,partition[i].length());
								}
								if(partition[i].contains("(")==true && part.length()>0)
								{
									int index=partition[i].indexOf("(");
									if(index==0)
									partition[i]=partition[i].substring(1,partition[i].length());
								}
								if(partition[i].contains("|")==true && part.length()>0)
								{
									int index=partition[i].indexOf("|");
									if(index==0)
									partition[i]=partition[i].substring(1,partition[i].length());
								}
								if(partition[i].contains("||")==true && part.length()>0)
								{
									int index=partition[i].indexOf("||");
									if(index==0)
									partition[i]=partition[i].substring(2,partition[i].length());
								}
								
								if(partition[i].contains("(")==true && part.length()>0 && partition[i].contains(")")==true)
								{
									int index=partition[i].indexOf("(");
									int index1=partition[i].indexOf(")");
									if(index==0 && index1==partition[i].length()-1)
									partition[i]=partition[i].substring(1,partition[i].length()-1);
								}
								
								
								if(partition[i].contains("||")==true && part.length()>0 && partition[i].contains("||")==true)
								{
									int index=partition[i].indexOf("||");
									int index1=partition[i].lastIndexOf("||");
									if(index==0 && index1==partition[i].length()-1)
									partition[i]=partition[i].substring(1,partition[i].length()-1);
								}
								if(partition[i].contains("[")==true && part.length()>0 && partition[i].contains("]")==true)
								{
									int index=partition[i].indexOf("[");
									int index1=partition[i].indexOf("]");
									if(index==0 && index1==partition[i].length()-1)
									partition[i]=partition[i].substring(1,partition[i].length()-1);
								}
								if(partition[i].contains("[")==true && part.length()>0)
								{
									int index=partition[i].indexOf("[");
									if(index==0)
									partition[i]=partition[i].substring(1,partition[i].length());
								}
								if(partition[i].contains("]")==true && part.length()>0)
								{
									int index=partition[i].lastIndexOf("]");
									if(index==0)
									partition[i]=partition[i].substring(1,partition[i].length());
								}

								if(partition[i].contains(":")==true && part.length()>0)
								{
									int index=partition[i].indexOf(":");
									partition[i]=partition[i].substring(0,index);
								}

								if(partition[i].contains("\"")==true && part.length()>0)
								{
									int index=partition[i].indexOf("\"");
									//System.out.println(index);
									partition[i]=partition[i].substring(0,index);
								}
								
								if(partition[i].contains("-")==true && part.length()>0)
								{
									int index=partition[i].indexOf("-");
									partition[i]=partition[i].substring(0,index);
								} 
								if(partition[i]!=null && partition[i]!=" ")
									
									for(int j=0;j<array.length;j++)
									{
										if(partition[i].equalsIgnoreCase(array[j])==true)
										c=1;
									}
									if(c!=1)
									{
										text_array[shift]=partition[i];
										shift++;
									}
							}
						}
						String val=+docid+","+line_no;
						//String val=""+line_no;
						for(int i=0;i<text_array.length && (text_array[i]!=null && !text_array[i].isEmpty() && text_array[i].trim().length() != 0);i++)
						{
							txtMapOutputKey.set(text_array[i].toLowerCase());
                            txtMapOutputValue.set(val);
							//System.out.println(text_array[i]+","+val);
							context.write(txtMapOutputKey, txtMapOutputValue);
						}
                    }
                }
            }
            catch(Exception e)
            {
                System.err.println(e+"Error inside Map....!!");
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text>//MapReduceBase implements Reducer<Text, Text, Text, Text>
    {
        private Text txtReduceOutputKey = new Text("");
        private Text txtReduceOutputValue = new Text("");
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
                
            //float tempSum[] = new float[300];//{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
           // int cnt[] = new int[300];
			//for(int i=0;i<300;i++)
		//	cnt[i]=0;

            //float preciSum[] = new float[12];//{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
            //int preciCnt[] = new int[12];
          
			int doc_id=0, lineNo=0, visited_docid=0, visited_lineNo=0;
			String value="";
            for(Text val : values)
            {
                String[] strElements = val.toString().split(",");
                doc_id = Integer.parseInt(strElements[0]);
                lineNo = Integer.parseInt(strElements[1]);
				if(!(visited_docid == doc_id && visited_lineNo==lineNo))
				{
					value += ":"+ lineNo;
					visited_docid=doc_id;
					visited_lineNo=lineNo;
				}
				//cnt[doc_id-1]++;
            }
			String str="";
			//for(int i=0;i<299;i++)
			//str += cnt[i]+",";	

			//str+= cnt[299];
			
			str += value;
			
            txtReduceOutputKey.set("\""+key.toString().toLowerCase()+"\"");
            txtReduceOutputValue.set(str);
            //output.collect(key, new Text(strOut));
            context.write(txtReduceOutputKey, txtReduceOutputValue);
        }
    }

 

//JOB 2 implementaion *****************************************

     public static class Map2 extends Mapper<LongWritable, Text, Text, Text> //MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
    {

     //   private BufferedReader brReader;
       // private String strStateName = "";
        private Text txtMapOutputKey1 = new Text("");
        private Text txtMapOutputValue1 = new Text(""); 
		
		
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
           // brReader = new BufferedReader(new FileReader(eachPath.toString()));
           // strLineRead = brReader.readLine();
			try
            {
                if(value != null)
                {
                		String line=value.toString();  
						String[] parts= line.split("&!@");
						int docid = Integer.parseInt(parts[0]);
						int line_no=Integer.parseInt(parts[1]);
						int shift=0;
						String text = parts[4];
						String hash[]=new String [10];
						if(parts[parts.length-1].contains(":")==false)
						{
							String hashtags=parts[parts.length-1];
							String hash_partition[]=hashtags.split(" +");
							if(text.contains("RT")==true)
							{
								for(int i=0;i<hash_partition.length/2;i++)
								if( hash_partition[i].contains("\\")==false)
								hash[shift]=hash_partition[i];
								shift++;
								//System.out.println(i+" "+hash_partition[i]);
							}
							else 
							{
								for(int i=0;i<hash_partition.length;i++)
								if( hash_partition[i].contains("\\")==false)
								hash[shift]=hash_partition[i];
								shift++;
							}
							//System.out.println();
						}	
            
					
						String val=+docid+","+line_no;
						for(int i=0;i<hash.length && (hash[i]!=null);i++)
						{
							txtMapOutputKey1.set(hash[i].toLowerCase());
                            txtMapOutputValue1.set(val);
							//System.out.println(hash[i]);
							context.write(txtMapOutputKey1, txtMapOutputValue1);
						}

				}
			}
            catch(Exception e)
            {
                System.err.println(e+"Error inside Map2....!!");
            }
        }
    }

    public static class Reduce2 extends Reducer<Text, Text, Text, Text>//MapReduceBase implements Reducer<Text, Text, Text, Text>
    {
        private Text txtReduceOutputKey = new Text("");
        private Text txtReduceOutputValue = new Text("");
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
			int doc_id=0, lineNo=0, visited_docid=0, visited_lineNo=0;
			String value="";
            for(Text val : values)
            {
                String[] strElements = val.toString().split(",");
                doc_id = Integer.parseInt(strElements[0]);
                lineNo = Integer.parseInt(strElements[1]);
				if(!(visited_docid == doc_id && visited_lineNo==lineNo))
				{
					value += ":"+ lineNo;
					visited_docid=doc_id;
					visited_lineNo=lineNo;
				}
            
			}
            txtReduceOutputKey.set("\""+key.toString().toLowerCase()+"\"");
            txtReduceOutputValue.set(value);
            //output.collect(key, new Text(strOut));
            context.write(txtReduceOutputKey, txtReduceOutputValue);
        }
    }
	
	
	
	//JOB 3 implementaion *****************************************

    public static class Map3 extends Mapper<LongWritable, Text, Text, Text> //MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
    {

       // private HashMap<String, String> StateMap = new HashMap<String, String>();
        private BufferedReader brReader;
        private String strStateName = "";
        private Text txtMapOutputKey = new Text("");
        private Text txtMapOutputValue = new Text(""); 
		
		
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
           // brReader = new BufferedReader(new FileReader(eachPath.toString()));
           // strLineRead = brReader.readLine();
			try
            {
            
                   String line=value.toString();  
					//System.out.println(line);
					int docid=0;
					int line_no=0;;
					String text;
					int shift=0;
					int c=0;
					
					if(line != null && !line.isEmpty())
                    {
String parts[]=line.split("&!@");
						docid = Integer.parseInt(parts[0]);
						line_no=Integer.parseInt(parts[1]);
						text = parts[4];
						String name=parts[6];
						int shift1=0;
							String name1[]=new String [30]; 
						if(text.contains("RT")==true && parts.length>16 && parts[16].contains("\\")==false && parts[16].contains(".")==false)
						{
							String name_t=parts[16];
							
				
							if(name_t.contains(" ")==true)
							{
								String [] split=name_t.split(" ");
								if(split.length>1)
								{
									//System.out.println("going in outside if");
									for(int j=0;j<split.length;j++)
									{
										//System.out.println("going in for");
										if(split[j].contains(",")==true)
										{	
											//System.out.println("going in");
											int index=split[j].indexOf(",");
											if(index==split[j].length()-1)
											split[j]=split[j].substring(0,split[j].length()-1);
										}
										if(split[j].contains("'")==true)
										{
											//System.out.println("going in");
											int index=split[j].indexOf("'");
											if(index==split[j].length()-1)
											split[j]=split[j].substring(0,split[j].length()-1);
										}
										if(split[j].contains("'")==true)
										{
											//System.out.println("going in");
											int index=split[j].indexOf("'");
											if(index==0)
											split[j]=split[j].substring(1,split[j].length());
										}
										if(split[j].contains("!")==true)
										{
											//System.out.println("going in");
											int index=split[j].indexOf("!");
											if(index==split[j].length()-1)
											split[j]=split[j].substring(0,split[j].length()-1);
										}
										if(split[j].contains("#")==true)
										{
											//System.out.println("going in");
											int index=split[j].indexOf("#");
											if(index==0)
											split[j]=split[j].substring(1,split[j].length());
										}
										if(split[j].contains("(")==true && split[j].contains(")")==true)
										{
											int index=split[j].indexOf("(");
											int index1=split[j].indexOf(")");
											if(index==0 && index1==split[j].length()-1)
											split[j]=split[j].substring(1,split[j].length()-1);
											else if (index!=0 && index1==split[j].length()-1)
											{
												split[j]=split[j].substring(0,index)+split[j].substring(index+1,split[j].length()-1);
											}
										}
										name1[shift1]=split[j];
										shift1++;
									}
								}
								else
								{
									if(name_t.contains(",")==true)
									{
										int index=name_t.indexOf(",");
										if(index==name_t.length()-1)
										name_t=name_t.substring(0,name_t.length()-1);
									}
									name1[shift1]=name_t;
									shift1++;
								}
							}
							else
							{
								if(name_t.contains(",")==true)
								{
									int index=name_t.indexOf(",");
									if(index==name_t.length()-1)
									name_t=name_t.substring(0,name_t.length()-1);
								}
								name1[shift1]=name_t;
								shift1++;
							}
						
						}
						if(name.contains("\\")==false && name.contains(".")==false )	
						{
							//name1[shift1]=name;
							if(name.contains(" ")==true)
							{
								String split1[]= name.split(" ");
								if(split1.length>1)
								{
									//System.out.println("going in outside if");
									for(int j=0;j<split1.length;j++)
									{
										//System.out.println("going in for");
										if(split1[j].contains(",")==true)
										{	
											//System.out.println("going in");
											int index=split1[j].indexOf(",");
											if(index==split1[j].length()-1)
											split1[j]=split1[j].substring(0,split1[j].length()-1);
										}
										if(split1[j].contains("'")==true)
										{
											//System.out.println("going in");
											int index=split1[j].indexOf("'");
											if(index==split1[j].length()-1)
											split1[j]=split1[j].substring(0,split1[j].length()-1);
										}
										if(split1[j].contains("'")==true)
										{
											//System.out.println("going in");
											int index=split1[j].indexOf("'");
											if(index==0)
											split1[j]=split1[j].substring(1,split1[j].length());
										}
										if(split1[j].contains("!")==true)
										{
											//System.out.println("going in");
											int index=split1[j].indexOf("!");
											if(index==split1[j].length()-1)
											split1[j]=split1[j].substring(0,split1[j].length()-1);
										}
										if(split1[j].contains("#")==true)
										{
											//System.out.println("going in");
											int index=split1[j].indexOf("#");
											if(index==0)
											split1[j]=split1[j].substring(1,split1[j].length());
										}
										if(split1[j].contains("(")==true && split1[j].contains(")")==true)
										{
											int index=split1[j].indexOf("(");
											int index1=split1[j].indexOf(")");
											if(index==0 && index1==split1[j].length()-1)
											split1[j]=split1[j].substring(1,split1[j].length()-1);
											else if (index!=0 && index1==split1[j].length()-1)
											{
												split1[j]=split1[j].substring(0,index)+split1[j].substring(index+1,split1[j].length()-1);
											}
										}
										name1[shift1]=split1[j];
										shift1++;
									}
								}
								else
								{
									if(name.contains(",")==true)
									{
										int index=name.indexOf(",");
										if(index==name.length()-1)
										name=name.substring(0,name.length()-1);
									}
									name1[shift1]=name;
									shift1++;
								}
							}
							else
							{
								if(name.contains(",")==true)
								{
									int index=name.indexOf(",");
									if(index==name.length()-1)
									name=name.substring(0,name.length()-1);
								}
								name1[shift1]=name;
								shift1++;
							}
							shift1++;
						}
						
						//System.out.println(name);
						String val=+docid+","+line_no;
						for(int i=0;i<name1.length && (name1[i]!=null && !name1[i].isEmpty() && name1[i].trim().length() != 0);i++)   /// addded new
						{
							txtMapOutputKey.set(name1[i].toLowerCase());
							txtMapOutputValue.set(val);
							//System.out.println(name1[i]);
							context.write(txtMapOutputKey, txtMapOutputValue);
						}
					}
					
					
			}
            catch(Exception e)
            {
                System.err.println(e+"Error inside Map3....!!");
            }
        }
    }

    public static class Reduce3 extends Reducer<Text, Text, Text, Text>//MapReduceBase implements Reducer<Text, Text, Text, Text>
    {
        private Text txtReduceOutputKey = new Text("");
        private Text txtReduceOutputValue = new Text("");
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
			int doc_id=0, lineNo=0, visited_docid=0, visited_lineNo=0;
			String value="";
            for(Text val : values)
            {
                String[] strElements = val.toString().split(",");
                doc_id = Integer.parseInt(strElements[0]);
                lineNo = Integer.parseInt(strElements[1]);
				
				if(!(visited_docid == doc_id && visited_lineNo == lineNo))
				{
					value += ":"+ lineNo;
					visited_docid=doc_id;
					visited_lineNo=lineNo;
				}
            }
            
            txtReduceOutputKey.set("\""+key.toString().toLowerCase()+"\"");
			txtReduceOutputValue.set(value);
            //output.collect(key, new Text(strOut));
            context.write(txtReduceOutputKey, txtReduceOutputValue);
        }
    }
	
	
	
	
	
	//JOB 4 implementaion *****************************************

     public static class Map4 extends Mapper<LongWritable, Text, Text, Text> //MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
    {

       // private HashMap<String, String> StateMap = new HashMap<String, String>();
        private BufferedReader brReader;
        private String strStateName = "";
        private Text txtMapOutputKey = new Text("");
        private Text txtMapOutputValue = new Text(""); 
		
		
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
           // brReader = new BufferedReader(new FileReader(eachPath.toString()));
           // strLineRead = brReader.readLine();
			try
            {
            
			   String line=value.toString();  
				//System.out.println(line);
				int docid=0;
				int line_no=0;;
				String text;
				int shift=0;
				int c=0;
				String hash[]=new String [10];
				
				//int shift1=0;
				
				if(line != null && !line.isEmpty())
				{
					int shift1=0;
					String parts[]=line.split("&!@");
					docid = Integer.parseInt(parts[0]);
					line_no=Integer.parseInt(parts[1]);
					text = parts[4];
					String screen_name=parts[7];
					String screen1[]=new String [5]; 
					
						
					if(text.contains("RT")==true && parts.length>17)
					{
						
						String screen_name_t=parts[17];
						if(screen_name_t.contains(" ")==true)
						{
							String [] split=screen_name_t.split(" ");
							if(split.length>1)
							{
								//System.out.println("going in outside if");
								for(int j=0;j<split.length;j++)
								{
									//System.out.println("going in for");
									if(split[j].contains(",")==true)
									{
										//System.out.println("going in");
										int index=split[j].indexOf(",");
										if(index==split[j].length()-1)
										split[j]=split[j].substring(0,split[j].length()-1);
									}
									screen1[shift1]=split[j];
									shift1++;
								}
							}
							else
							{
								if(screen_name_t.contains(",")==true)
								{
									int index=screen_name_t.indexOf(",");
									if(index==screen_name_t.length()-1)
									screen_name_t=screen_name_t.substring(0,screen_name_t.length()-1);
								}
								screen1[shift1]=screen_name_t;
								shift1++;
							}
						}
						else
						{
							if(screen_name_t.contains(",")==true)
							{
								int index=screen_name_t.indexOf(",");
								if(index==screen_name_t.length()-1)
								screen_name_t=screen_name_t.substring(0,screen_name_t.length()-1);
							}
							screen1[shift1]=screen_name_t;
							shift1++;
						}	
		
					}
					screen1[shift1]=screen_name;
					String val=+docid+","+line_no;
					for(int i=0;i<screen1.length && (screen1[i]!=null && !screen1[i].isEmpty() && screen1[i].trim().length() != 0);i++)   /// addded new
					{
						txtMapOutputKey.set(screen1[i].toLowerCase());
						txtMapOutputValue.set(val);
						//System.out.println(screen1[i]);
						context.write(txtMapOutputKey, txtMapOutputValue);
					}
				
				}
			}
            catch(Exception e)
            {
                System.err.println(e+"Error inside Map4....!!");
            }
        }
    }

    public static class Reduce4 extends Reducer<Text, Text, Text, Text>//MapReduceBase implements Reducer<Text, Text, Text, Text>
    {
        private Text txtReduceOutputKey = new Text("");
        private Text txtReduceOutputValue = new Text("");
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
			int doc_id=0, lineNo=0, visited_docid=0, visited_lineNo=0;
			String value="";
            for(Text val : values)
            {
                String[] strElements = val.toString().split(",");
                doc_id = Integer.parseInt(strElements[0]);
                lineNo = Integer.parseInt(strElements[1]);
				if(!(visited_docid == doc_id && visited_lineNo==lineNo))
				{
					value += ":"+ lineNo;
					visited_docid=doc_id;
					visited_lineNo=lineNo;
				}
            }
          
			txtReduceOutputKey.set("\""+key.toString().toLowerCase()+"\"");
            txtReduceOutputValue.set(value);
            //output.collect(key, new Text(strOut));
            context.write(txtReduceOutputKey, txtReduceOutputValue);
        }
    }

  public static void main(String[] args) throws Exception 
  {

  Job job = new Job();
    Configuration conf = job.getConfiguration();
    job.setJobName("textIndex");
    
   job.setJarByClass(text_index.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(Map.class);
	//job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    
    //job.setNumReduceTasks(0);
    boolean success = job.waitForCompletion(true); 
	
		//job2 configuration ************************************************
	    Job job2 = new Job();
	    Configuration conf2 = job2.getConfiguration();
	    job2.setJobName("textIndex");
	   
	    job2.setJarByClass(text_index.class);
	    
	    FileInputFormat.addInputPath(job2, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job2, new Path(args[2]));

	    job2.setMapperClass(Map2.class);
		//job.setCombinerClass(Reduce.class);
	    job2.setReducerClass(Reduce2.class);

	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(Text.class);
	    
	    
	    //job.setNumReduceTasks(0);
	    boolean success2 = job2.waitForCompletion(true); 
	
	//job3 configuration for names************************************************
	
	
		Job job3 = new Job();
	    Configuration conf3 = job3.getConfiguration();
	    job3.setJobName("names");
	   
	    job3.setJarByClass(text_index.class);
	    
	    FileInputFormat.addInputPath(job3, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job3, new Path(args[3]));

	    job3.setMapperClass(Map3.class);
		//job.setCombinerClass(Reduce.class);
	    job3.setReducerClass(Reduce3.class);

	    job3.setOutputKeyClass(Text.class);
	    job3.setOutputValueClass(Text.class);
	    
	    
	    //job.setNumReduceTasks(0);
	    boolean success3 = job3.waitForCompletion(true); 
	
	
	//job4 configuration for the screen name ************************************************
	Job job4 = new Job();
	    Configuration conf4 = job4.getConfiguration();
	    job4.setJobName("screen_name");
	    
	    
	    job4.setJarByClass(text_index.class);
	    
	    FileInputFormat.addInputPath(job4, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job4, new Path(args[4]));

	    job4.setMapperClass(Map4.class);
		//job.setCombinerClass(Reduce.class);
	    job4.setReducerClass(Reduce4.class);

	    job4.setOutputKeyClass(Text.class);
	    job4.setOutputValueClass(Text.class);
	    
	    
	    //job.setNumReduceTasks(0);
	    boolean success4 = job4.waitForCompletion(true); 

  }
}


