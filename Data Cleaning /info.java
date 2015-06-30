import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
class info
{
    public static void main(String args[])throws IOException
    {
        String line,s="",str;
		int z=0;
		int xyz=0;
		for(int ky=0;ky<300;ky++)
		{	
			FileWriter fw = new FileWriter("clean/tweets_test"+(ky+1)+".txt",true);
			BufferedWriter bw = new BufferedWriter(fw);
			System.out.println("Creating file "+((ky*1))+"...........");
			for(int i=1; i<=10;i++)
			{
				BufferedReader br = new BufferedReader(new FileReader("Data/tweets_test"+((ky*10)+i)+".txt"));
				line = br.readLine();
				
				// if the line is null then it takes care of it 
				System.out.println("Reading file "+((ky*10)+i)+"...........");
				if(line==null)
				{
					line = br.readLine();
				}
				/**
				************************
				The loop for the each line in  the file
				*/
				
				outer: while ((line) != null)
				{
					String finalhash="";
					String[] parts = line.split(",");
					String sub="";
					String id="";
					String text="";
					String tweet_post="";
					String name="";
					String screen_name="";
					int followers=0;
					int friends=0;
					int status_count=0;
					String created_acc="";
					String re_id="";
					String re_text="";
					String re_tweet_post="";
					String re_name="";
					String re_screen_name="";
					int re_followers=0;
					int re_friends=0;
					int re_status_count=0;
					String re_created_acc="";
					String tweet_time="";
					String tweet_id="";
					String re_tweet_time="";
					String re_tweet_id="";
					if(parts.length>1)
					{
						int co=0;
						inner: for(int k=0;k<parts.length;k++)
						{
							String check="";
							String check1="";
							check = parts[k];
							
							if(k==0 && co==0 && check.length()>15)
							{
								tweet_id=check.substring(2,12);
								tweet_time=check.substring(15,check.length()-1);
							}
							
							else
							{	
								int index=0;
								for(int j=1;j<check.length();j++)
								{
									if(check.charAt(j) ==  '"' && j<check.length()-1)
									{
										check1= check.substring(index+1,j);
										sub=check.substring(j+2,check.length());
										break;	
									}			
								}
								
								if(parts[k].contains("{\"text\"")==true)
								{
									String hashtags=parts[k];
									
									if(hashtags.length()>10 && hashtags.length()<24)
									hashtags= hashtags.substring(9, hashtags.length()-1);
									if(hashtags.length()>33)
									hashtags= hashtags.substring(33, hashtags.length()-1);
									finalhash =hashtags+" "+finalhash;
									
								}
							}
							
							if(check1.equals("retweeted_status")==true && sub.length()>15)
							{
								
								re_tweet_time=sub.substring(15,sub.length()-1);
								co=1;
								continue inner;
								
							}
							
							else if(co==1)
							{
								if(check1.equals("text")==true && sub.length()>1)
								{
									sub=sub.substring(1,sub.length());
									String reg="http:";
									Pattern checkRegex=Pattern.compile(reg);
									Matcher regexMatcher = checkRegex.matcher(sub);
									int kx=0;
									while(regexMatcher.find() && kx!=1)
									{
										if(regexMatcher.group().length() !=0)
										regexMatcher.group().trim();
							
										int index = regexMatcher.end();
										kx++; 
										sub=sub.substring(0,index-5);
									}
								}
								
								else if((check1.equals("id_str")== true || check1.equals("name")== true || check1.equals("screen_name")== true || check1.equals("created_at")== true) && sub.length()>2  )
								{
									sub=sub.substring(1,sub.length()-1);
								}
								
								else if((check1.equals("id_str")== true || check1.equals("name")== true || check1.equals("screen_name")== true || check1.equals("created_at")== true )  && sub.length()>2 )
								{
									sub=sub.substring(0,sub.length()-1);
								}
								
								if(check1.equals("id")== true)
								re_id=sub;
								if(check1.equals("text")== true)
								re_text=sub;
								if(check1.equals("id_str")== true)
								re_tweet_post=sub;
								if(check1.equals("name")== true)
								re_name=sub;	
								if(check1.equals("screen_name")== true)
								re_screen_name=sub;
								if(check1.equals("followers_count")== true && sub.length()>2)
								re_followers=Integer.parseInt(sub);
								if(check1.equals("friends_count")== true && sub.length()>2)
								re_friends=Integer.parseInt(sub);
								if(check1.equals("statuses_count")== true && sub.length()>2)
								re_status_count=Integer.parseInt(sub);
								if(check1.equals("created_at")== true)
								re_created_acc=sub;
							}
							
							else
							{
								if(check1.equals("text")==true && sub.length()>1)
								{
									sub=sub.substring(1,sub.length());
									String reg="http:";
									Pattern checkRegex=Pattern.compile(reg);
									Matcher regexMatcher = checkRegex.matcher(sub);
									int kx=0;
									while(regexMatcher.find() && kx!=1)
									{
										if(regexMatcher.group().length() !=0)
										regexMatcher.group().trim();
							
										int index = regexMatcher.end();
										kx++; 
										sub=sub.substring(0,index-5);
									}
								}
								
								else if((check1.equals("id_str")== true || check1.equals("name")== true || check1.equals("screen_name")== true || check1.equals("created_at")== true) && sub.length()>2  )
								{
									sub=sub.substring(1,sub.length()-1);
								}
								
								else if(check1.equals("id_str")== true || check1.equals("name")== true || check1.equals("screen_name")== true || check1.equals("created_at")== true && sub.length()>1 )
								{
									sub=sub.substring(0,sub.length()-1);
								}
								
								if(check1.equals("id")== true)
								id=sub;
								if(check1.equals("text")== true)
								text=sub;
								if(check1.equals("id_str")== true)
								tweet_post=sub;
								if(check1.equals("name")== true)
								name=sub;	
								if(check1.equals("screen_name")== true)
								screen_name=sub;
								if(check1.equals("followers_count")== true && !(sub.equals(" ")))
								followers=Integer.parseInt(sub);
								if(check1.equals("friends_count")== true && !(sub.equals(" ")))
								friends=Integer.parseInt(sub);
								if(check1.equals("statuses_count")== true && !(sub.equals(" ")))
								status_count=Integer.parseInt(sub);
								if(check1.equals("created_at")== true)
								created_acc=sub;
							}
						}
						z++;
						
						String output= (ky+1)+"&!@"+xyz+"&!@"+tweet_time+"&!@"+id+"&!@"+text+"&!@"+tweet_post+"&!@"+name+"&!@"+screen_name+"&!@"+followers+"&!@"+friends+"&!@"+status_count+"&!@"+created_acc+"&!@"+finalhash;
						if(co==1)
						{
							z++;
							output += re_tweet_time+"&!@"+re_id+"&!@"+re_text+"&!@"+re_tweet_post+"&!@"+re_name+"&!@"+re_screen_name+"&!@"+re_followers+"&!@"+re_friends+"&!@"+re_status_count+"&!@"+re_created_acc+"&!@"+finalhash;
						}
						xyz++;
						bw.write(""+output);
						bw.write("\r\n");
					}	
					
					line = br.readLine();
					
					if(line!=null)
					{
						while(line.equals(" +"))
						line = br.readLine();
					}
					
				}
				br.close();	
			}
			bw.close();
		}		
    }
}       