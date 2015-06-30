import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
class final_value
{
    public static void main(String args[])throws IOException
    {
		
		int k=0;
		
		String line;
		for(int ky=136;ky<=300;ky++)
		{
		FileWriter fw = new FileWriter("database_values_original"+(ky+1)+".txt",true);
		BufferedWriter bw = new BufferedWriter(fw);
		BufferedReader br = new BufferedReader(new FileReader("clean/tweets_test"+(ky+1)+".txt"));
		//line = br.readLine();
	//	for(int i=0;i<3785;i++)
		//	line = br.readLine();
		while ((line = br.readLine()) != null)
		{
			//System.out.println(line);
			k++;
			
			String parts[]=line.split("&!@");
			//for(int i=0;i<parts.length;i++)
			//System.out.println(i+"  "+parts[i]);
			
			int tweetid=Integer.parseInt(parts[1]);
			String tweet_time=parts[2];
			//int ori_tweet_id=Integer.parseInt(parts[3]);
			String text= parts[4];
			String name=parts[6];
			String screen_name=parts[7];
			int followers=Integer.parseInt(parts[8]);
			int friends=Integer.parseInt(parts[9]);
			String re_name=" ";
			String re_screen=" ";
			String final_text="";
			if(name.contains("\"")==true && name.length()>0)
				{
					int index=name.indexOf("\"");
					if(name.length()<3)
					name="";
					else
					name=name.substring(1,index);
				}
			if(text.contains("\"")==true && text.length()>0)
			{
				int index=text.indexOf("\"");
				text=text.substring(0,index);
			}
			if(text.contains("\\")==true && text.length()>0)
			{
				int index=text.indexOf("\\");
				if(index==text.length()-1)
				text=text.substring(0,index);
			}
			if(text.contains("\\")==true && text.length()>0)
			{
				int index=text.indexOf("\\");
				if(index==text.length()-1)
				text=text.substring(0,index);
			}
			if(text.contains("\\")==true && text.length()>0)
			{
				int index=text.indexOf("\\");
				if(index==text.length()-1)
				text=text.substring(0,index);
			}
			if(parts.length>18)
			{
				re_name=parts[16];
				if(re_name.contains("\"\"")==true && re_name.length()>0)
				{
					re_name="";
				}
				if(re_name.contains("\"")==true && re_name.length()>0)
				{
					int index=re_name.indexOf("\"");
					if(re_name.length()<3)
					re_name="";
					else
					re_name=re_name.substring(1,index);
				}
				re_screen=parts[17];
				final_text= "insert into tweet values("+tweetid+",\""+tweet_time+"\",\""+text+"\","+"\""+name+"\","+"\""+screen_name+"\","+followers+","+friends+","+"\""+re_name+"\","+"\""+re_screen+"\");";
			
			}
			else
			{
			     final_text= "insert into tweet values("+tweetid+",\""+tweet_time+"\",\""+text+"\","+"\""+name+"\","+"\""+screen_name+"\","+followers+","+friends+","+"\""+re_name+"\","+"\""+re_screen+"\");";
				
			}
			
			bw.write(""+final_text);
			bw.write("\r\n");
			
		}
		br.close();
		bw.close();
		}
	}
}