<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
    
    
    <%@ page import ="java.io.*" %>
	<%@ page import ="java.nio.file.Paths" %>
	<%@ page import ="java.util.ArrayList" %>
	<%@ page import ="java.util.Date" %>
	<%@ page import ="java.text.DateFormat" %>
	<%@ page import ="java.text.SimpleDateFormat" %>
	<%@ page import ="java.io.IOException" %>
	<%@ page import ="java.util.StringTokenizer " %>

	    
    
    <%@ page import ="org.apache.lucene.analysis.standard.StandardAnalyzer" %>
	<%@ page import ="org.apache.lucene.document.Document" %>
	<%@ page import ="org.apache.lucene.document.Field" %>
	<%@ page import ="org.apache.lucene.document.StringField" %>
	<%@ page import ="org.apache.lucene.document.TextField" %>
	<%@ page import ="org.apache.lucene.index.DirectoryReader" %>
	<%@ page import ="org.apache.lucene.index.IndexReader" %>
	<%@ page import ="org.apache.lucene.index.IndexWriter" %>
	<%@ page import ="org.apache.lucene.index.IndexWriterConfig" %>
	<%@ page import ="org.apache.lucene.queryparser.classic.ParseException" %>
	<%@ page import ="org.apache.lucene.queryparser.classic.QueryParser" %>
	<%@ page import ="org.apache.lucene.search.IndexSearcher" %>
	<%@ page import ="org.apache.lucene.search.Query" %>
	<%@ page import ="org.apache.lucene.search.ScoreDoc" %>
	<%@ page import ="org.apache.lucene.search.TopScoreDocCollector" %>
	<%@ page import ="org.apache.lucene.store.Directory" %>
	<%@ page import ="org.apache.lucene.store.FSDirectory" %>
	<%@ page import ="org.apache.lucene.search.TopDocs" %>
	

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<style>
   <%@ include file="style1.css"%>
</style>
<title>TWEETS</title>
</head>
<body>
    
<% 
        StandardAnalyzer analyzer = new StandardAnalyzer();
        String s=request.getParameter("name");                                                      //to get the value passed from searchbox in 1st page


        IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get("C:\\Users\\Kimsuka\\Documents\\Index2")));
        IndexSearcher searcher = new IndexSearcher(reader);

        
       
          try {
           
            Query q = new QueryParser("contents", analyzer).parse(s);
            TopDocs results = searcher.search(q,200);
            ScoreDoc[] hits = results.scoreDocs;

            // 4. display results
            out.println("Found " + hits.length + " hits.");
            out.println("<BR>");  
            for(int i=0;i<hits.length;++i) {
              int docId = hits[i].doc;
              Document d = searcher.doc(docId);
              out.println((i + 1));
              out.println("<BR>");           
              out.println(d.get("name"));
              out.println("<BR>");
              out.println ("score=" + hits[i].score);  
              //similarly output all the fields you want to display by giving d.get("yourfieldname")
             out.println("<BR>"); 
            }
          } 
           catch (Exception e) 
           {
            out.println("Error searching " + s + " : " + e.getMessage());
          }
        
%>
</body>
</html>
