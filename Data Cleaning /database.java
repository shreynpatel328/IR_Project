import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
class database
{
    public static void main(String args[])throws IOException
    {
		
		int k=0;
		String line;
		FileWriter fw = new FileWriter("database_name_text.txt",true);
		BufferedWriter bw = new BufferedWriter(fw);
		BufferedReader br = new BufferedReader(new FileReader("temp_name_java.txt"));
		line = br.readLine();
		while ((line = br.readLine()) != null)
		{
			//System.out.println(line);
			k++;
			
			String parts[]=line.split(":");
			//for(int i=0;i<parts.length;i++)
			//System.out.println(parts[i]);
			String part=parts[0];
			int index=part.lastIndexOf("\"");
			//partition[i]=partition[i].substring(0,index);
			String line_no="";
			for(int i=1;i<parts.length-1;i++)
			line_no += parts[i]+":";
			line_no +=parts[parts.length-1];
			String final_text= "insert into index_name values("+part.substring(0,index+1)+",\""+part.substring(index+2,part.length())+line_no+"\");";
			//System.out.println(final_text);
			bw.write(""+final_text);
			bw.write("\r\n");
		}
		br.close();
		bw.close();
		
		/*
		int k=0;
		String line;
		FileWriter fw = new FileWriter("database_values.txt",true);
		BufferedWriter bw = new BufferedWriter(fw);
		BufferedReader br = new BufferedReader(new FileReader("hash.txt"));
		line = br.readLine();
		while ((line = br.readLine()) != null)
		{
			//System.out.println(line);
			k++;
			
			String parts[]=line.split(":");
			//for(int i=0;i<parts.length;i++)
			//System.out.println(parts[i]);
			String part=parts[0];
			int index=part.lastIndexOf("\"");
			//partition[i]=partition[i].substring(0,index);
			String line_no="";
			for(int i=1;i<parts.length-1;i++)
			line_no += parts[i]+":";
			line_no +=parts[parts.length-1];
			String final_text= "insert into index_hash values("+part.substring(0,index+1)+",\""+part.substring(index+2,part.length())+line_no+"\");";
			//System.out.println(final_text);
			bw.write(""+final_text);
			bw.write("\r\n");
		}
		br.close();
		bw.close();
		/*
		int k=0;
		String line;
		FileWriter fw = new FileWriter("database_values.txt",true);
		BufferedWriter bw = new BufferedWriter(fw);
		BufferedReader br = new BufferedReader(new FileReader("name.txt"));
		line = br.readLine();
		while ((line = br.readLine()) != null)
		{
			//System.out.println(line);
			k++;
			
			String parts[]=line.split("&!@");
			//for(int i=0;i<parts.length;i++)
			//System.out.println(parts[i]);
			String part=parts[0];
			int index=part.lastIndexOf("\"");
			//partition[i]=partition[i].substring(0,index);
			String line_no="";
			for(int i=1;i<parts.length-1;i++)
			line_no += parts[i]+":";
			line_no +=parts[parts.length-1];
			String final_text= "insert into index_name values("+part.substring(0,index+1)+",\""+part.substring(index+2,part.length())+line_no+"\");";
			//System.out.println(final_text);
			bw.write(""+final_text);
			bw.write("\r\n");
		}
		br.close();
		bw.close();
		*/
		/*
		int k=0;
		String line;
		FileWriter fw = new FileWriter("database_values.txt",true);
		BufferedWriter bw = new BufferedWriter(fw);
		BufferedReader br = new BufferedReader(new FileReader("screen.txt"));
		line = br.readLine();
		while ((line = br.readLine()) != null)
		{
			//System.out.println(line);
			k++;
			
			String parts[]=line.split("&!@");
			//for(int i=0;i<parts.length;i++)
			//System.out.println(parts[i]);
			String part=parts[0];
			int index=part.lastIndexOf("\"");
			//partition[i]=partition[i].substring(0,index);
			String line_no="";
			for(int i=1;i<parts.length-1;i++)
			line_no += parts[i]+":";
			line_no +=parts[parts.length-1];
			String final_text= "insert into index_screen values("+part.substring(0,index+1)+",\""+part.substring(index+2,part.length())+line_no+"\");";
			//System.out.println(final_text);
			bw.write(""+final_text);
			bw.write("\r\n");
		}
		br.close();
		bw.close();
	*/
		
	}
}