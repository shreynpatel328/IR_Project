library(httr)
install.packages("streamR")
library(streamR)
id = 1:400
for(i in id)
{
  fileName=print(paste0("tweets_test", i,".txt"))
  filterStream( file.name=fileName,track="twitter", tweets=10000, oauth=twitCred, timeout=1000, lang='en') 
} 