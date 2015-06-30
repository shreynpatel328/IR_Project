install.packages("RCur")
install.packages("ROAuth")
install.packages("stringr")
install.packages("twitteR")
library(RCurl)
library(ROAuth)
library(twitteR)
library(stringr)
options(RCurlOptions = list(cainfo = system.file("CurlSSL", "cacert.pem", package = "RCurl")))
accessURL <- "https://api.twitter.com/oauth/access_token"
authURL <- "https://api.twitter.com/oauth/authorize"
reqURL="https://api.twitter.com/oauth/request_token"
consumerKey = "PHOfBbEy34sS0bnIqRmLd9RMq"
consumerSecret = "JZ6WEyZO5w0AEUnriT6wU8FhTtQedSDuCfFl1s5Taghj4okmkd"
twitCred <- OAuthFactory$new(consumerKey=consumerKey, consumerSecret=consumerSecret, requestURL=reqURL, accessURL=accessURL,authURL=authURL)
twitCred$handshake()