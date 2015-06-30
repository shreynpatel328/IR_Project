package lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Date;
import java.io.*;

/** Index all text files under a directory.
 * <p>
 * This is a command-line application demonstrating simple Lucene indexing.
 * Run it with no command-line arguments for usage information.
 */
public class IndexFiles {
  
  private IndexFiles() {}

  /** Index all text files under a directory. */
  public static void main(String[] args) {
  // String usage = "java org.apache.lucene.demo.IndexFiles"
    //            + " [-index INDEX_PATH] [-docs DOCS_PATH] [-update]\n\n"
     //          + "This indexes the documents in DOCS_PATH, creating a Lucene index"
       //        + "in INDEX_PATH that can be searched with SearchFiles";
    String indexPath = "C:\\Users\\Sam\\Documents\\index";
    String docsPath = "C:\\Users\\Sam\\Documents\\Data";
   boolean create = true;
   /* for(int i=0;i<args.length;i++) {
      if ("-index".equals(args[i])) {
        indexPath = args[i+1];
       i++;
     } else if ("-docs".equals(args[i])) {
       docsPath = args[i+1];
        i++;
      } else if ("-update".equals(args[i])) {
        create = false;
      }
    }

    if (docsPath == null) {
      System.err.println("Usage: " + usage);
      System.exit(1);
   }*/
    final Path docDir = Paths.get(docsPath);
  if (!Files.isReadable(docDir)) {
      System.out.println("Document directory '" +docDir.toAbsolutePath()+ "' does not exist or is not readable, please check the path");
      System.exit(1);
    }
    
    Date start = new Date();
    try {
      System.out.println("Indexing to directory '" + indexPath + "'...");

    Directory dir = FSDirectory.open(Paths.get(indexPath));
      Analyzer analyzer = new StandardAnalyzer();
      IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

    if (create) {
      // Create a new index in the directory, removing any
      // previously indexed documents:
       iwc.setOpenMode(OpenMode.CREATE);
      } else {
       // Add new documents to an existing index:
        iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
     }
      // Optional: for better indexing performance, if you
     // are indexing many documents, increase the RAM
      // buffer.  But if you do this, increase the max heap
     // size to the JVM (eg add -Xmx512m or -Xmx1g):
     //
      // iwc.setRAMBufferSizeMB(256.0);

      IndexWriter writer = new IndexWriter(dir, iwc);
     indexDocs(writer, docDir);

     // NOTE: if you want to maximize search performance,
      // you can optionally call forceMerge here.  This can be
    // a terribly costly operation, so generally it's only
      // worth it when your index is relatively static (ie
      // you're done adding documents to it):
      //
     // writer.forceMerge(1);

      writer.close();
      Date end = new Date();
      System.out.println(end.getTime() - start.getTime() + " total milliseconds");

    } catch (IOException e) {
      System.out.println(" caught a " + e.getClass() +
      "\n with message: " + e.getMessage());
   }
  }
  /**
   * Indexes the given file using the given writer, or if a directory is given,
   * recurses over files and directories found under the given directory.
  * 
   * NOTE: This method indexes one document per input file.  This is slow.  For good
   * throughput, put multiple documents into your input file(s).  An example of this is
  * in the benchmark module, which can create "line doc" files, one document per line,
 * using the
   * <a href="../../../../../contrib-benchmark/org/apache/lucene/benchmark/byTask/tasks/WriteLineDocTask.html"
   * >WriteLineDocTask</a>.
   *  
   * @param writer Writer to the index where the given file/dir info will be stored
  * @param path The file to index, or the directory to recurse into to find files to index
   * @throws IOException If there is a low-level I/O error
  */
  static void indexDocs(final IndexWriter writer, Path path) throws IOException {
    if (Files.isDirectory(path)) {
     Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
        @Override
       public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          try {
            indexDoc(writer, file, attrs.lastModifiedTime().toMillis());
         } catch (IOException ignore) {
           // don't index files that can't be read.
          }
        return FileVisitResult.CONTINUE;
       }
      });
  } else {
      indexDoc(writer, path, Files.getLastModifiedTime(path).toMillis());
   }
 }
  /** Indexes a single document */
  static void indexDoc(IndexWriter writer, Path file, long lastModified) throws IOException {
   try (InputStream stream = Files.newInputStream(file)) {
      // make a new, empty document
     Document doc = new Document();
     
    BufferedReader br = new BufferedReader(new FileReader(file.toString()));
	 String line = br.readLine();
	// System.out.println(line);
	 if(line==null)
		{
			line = br.readLine();
		}
	 outer: while ((line) != null)
		{
			String[] parts = line.split(",");
			String sub="";
			String id="";
			String text="";
			String tweet_post="";
			String name="";
			String hastags="";
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
			int co=0;
			if(parts.length>1)
			{
				
				inner: for(int k=0;k<parts.length;k++)
				{
					String check="";
					String check1="";
					check = parts[k];
					if(k==0 && co==0)
					{
						tweet_id=check.substring(2,12);
						tweet_time=check.substring(15,check.length()-1);
						//System.out.println(tweet_id+": "+ tweet_time);
					}
					else
					{	
						int index=0;
						for(int j=1;j<check.length();j++)
						{
							if(check.charAt(j) ==  '"' && j<check.length()-1)
							{
								//System.out.println(check);
								check1= check.substring(index+1,j);
								sub=check.substring(j+2,check.length());
								break;	
							}			
						}
						//System.out.println(check1);
					}
					if(check1.equals("retweeted_status")==true && sub.length()>15)
					{
						
						//re_tweet_id=sub.substring(2,12);
						re_tweet_time=sub.substring(15,sub.length()-1);
						
						co=1;
						continue inner;
						
					}
					else if(co==1)
					{
						if(check1.equals("text")==true)
						{
							//System.out.println("going inside");
							//System.out.println(s);
							sub=sub.substring(1,sub.length());
							String reg="http:";
							String reg1="\\";
							Pattern checkRegex=Pattern.compile(reg);
							Matcher regexMatcher = checkRegex.matcher(sub);
							int kx=0;
							while(regexMatcher.find() && kx!=1)
							{
								if(regexMatcher.group().length() !=0)
								regexMatcher.group().trim();
					
								int index = regexMatcher.end();
								kx++; 
								//System.out.println("end intex : "+index);
								sub=sub.substring(0,index-5);
								//System.out.println("Tweet of the person is : "+str);
							}
							
						}
						else if((check1.equals("id_str")== true || check1.equals("name")== true || check1.equals("hastags")== true || check1.equals("created_at")== true) && sub.length()>2  )
						{
							sub=sub.substring(1,sub.length()-1);
						}
						else if((check1.equals("id_str")== true || check1.equals("name")== true || check1.equals("hastags")== true || check1.equals("created_at")== true) &&  sub.length()>2)
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
						if(check1.equals("hastags")== true)
						re_screen_name=sub;
						if(check1.equals("followers_count")== true)
						re_followers=Integer.parseInt(sub);
						if(check1.equals("friends_count")== true)
						re_friends=Integer.parseInt(sub);
						if(check1.equals("statuses_count")== true)
						re_status_count=Integer.parseInt(sub);
						if(check1.equals("created_at")== true)
						re_created_acc=sub;
						
					}
					else
					{
						if(check1.equals("text")==true)
						{
							//System.out.println("going inside");
							//System.out.println(s);
							sub=sub.substring(1,sub.length());
							String reg="http:";
							String reg1="\\";
							Pattern checkRegex=Pattern.compile(reg);
							Matcher regexMatcher = checkRegex.matcher(sub);
							int kx=0;
							while(regexMatcher.find() && kx!=1)
							{
								if(regexMatcher.group().length() !=0)
								regexMatcher.group().trim();
					
								int index = regexMatcher.end();
								kx++; 
								//System.out.println("end intex : "+index);
								sub=sub.substring(0,index-5);
								//System.out.println("Tweet of the person is : "+str);
							}
							
						}
						else if((check1.equals("id_str")== true || check1.equals("name")== true || check1.equals("hastags")== true || check1.equals("created_at")== true) && sub.length()>2  )
						{
							sub=sub.substring(1,sub.length()-1);
						}
						else if(check1.equals("id_str")== true || check1.equals("name")== true || check1.equals("hastags")== true || check1.equals("created_at")== true  )
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
						hastags=sub;
						if(check1.equals("followers_count")== true)
						followers=Integer.parseInt(sub);
						if(check1.equals("friends_count")== true)
						friends=Integer.parseInt(sub);
						if(check1.equals("statuses_count")== true)
						status_count=Integer.parseInt(sub);
						if(check1.equals("created_at")== true)
						created_acc=sub;
					}
				}
				
				/*
				System.out.println("id: "+id);
				System.out.println("text: "+text);
				System.out.println("id_str: "+tweet_post);
				System.out.println("name: "+name);
				System.out.println("screen_name: "+screen_name);
				System.out.println("foloowers: "+followers);
				System.out.println("freidsn: "+friends);
				System.out.println("status: "+status_count);
				System.out.println("create at: "+created_acc);
				if(co==1)
				{
					
					System.out.println();
					System.out.println("RETWEET");
					System.out.println(re_tweet_id+": "+ re_tweet_time);
					System.out.println("id: "+re_id);
					System.out.println("text: "+re_text);
					System.out.println("id_str: "+re_tweet_post);
					System.out.println("name: "+re_name);
					System.out.println("screen_name: "+re_screen_name);
					System.out.println("foloowers: "+re_followers);
					System.out.println("freidsn: "+re_friends);
					System.out.println("status: "+re_status_count);
					System.out.println("create at: "+re_created_acc);
				}
				System.out.println();
				System.out.println();
				*/
			
			}	
			
			
			line = br.readLine();
			//line = br.readLine();
			if(line!=null)
			{
				while(line.equals(" +"))
				//while(Strings.isNullOrEmpty(line))
				line = br.readLine();
				//line = br.readLine();			
			}
			
			String combine="";
			if(co==1)
			{
				// combine=tweet_time+" "+id+" "+text+" "+tweet_post+" "+followers+" "+friends+" "+created_acc+" "+re_tweet_time+" "+re_name+" "+re_screen_name+" "+re_id+" "+re_text+" "+re_tweet_post+" "+re_followers+" "+re_friends+" "+re_created_acc;
				
			}
			else 
			{
				combine=tweet_time+" "+id+" "+text+" "+tweet_post+" "+followers+" "+friends+" "+created_acc;
			}
			//System.out.println(" the total no of tweet is "+ z);
			// Field pathField = new StringField("combine", file.toString(), Field.Store.YES);
		    // Field combine = 
		    
		    doc.add(new Field("combine", combine, Field.Store.YES, Field.Index.ANALYZED));
		    doc.add(new Field("Name", name, Field.Store.YES, Field.Index.ANALYZED));
		    doc.add(new Field("hastags", hastags, Field.Store.YES, Field.Index.ANALYZED));
			//	
		}
      // Add the path of the file as a field named "path".  Use a
     // field that is indexed (i.e. searchable), but don't tokenize 
      // the field into separate words and don't index term frequency
      // or positional information:
     
    
     
      // Add the last modified date of the file a field named "modified".
      // Use a LongField that is indexed (i.e. efficiently filterable with
      // NumericRangeFilter).  This indexes to milli-second resolution, which
      // is often too fine.  You could instead create a number based on
      // year/month/day/hour/minutes/seconds, down the resolution you require.
      // For example the long value 2011021714 would mean
      // February 17, 2011, 2-3 PM.
      doc.add(new LongField("modified", lastModified, Field.Store.NO));
      
      // Add the contents of the file to a field named "contents".  Specify a Reader,
      // so that the text of the file is tokenized and indexed, but not stored.
      // Note that FileReader expects the file to be in UTF-8 encoding.
     // If that's not the case searching for special characters will fail.
      doc.add(new TextField("contents", new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))));
      
     if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
        // New index, so we just add the document (no old document can be there):
        System.out.println("adding " + file);
        writer.addDocument(doc);
      } else {
        // Existing index (an old copy of this document may have been indexed) so 
        // we use updateDocument instead to replace the old one matching the exact 
        // path, if present:
        System.out.println("updating " + file);
        writer.updateDocument(new Term("path", file.toString()), doc);
      }
    }
  }
}
