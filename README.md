## Content crawl

This project contains crawler code to access the Mendeley API and Contentful API and index them in an Elastic Search index.  

### Mendeley Crawl

The Mendeley API requires an application key and secret which can be created by:

  1. Create an account on http://dev.mendeley.com/
  2. Clicking on the "My Apps" tab
  3. Creating an application, putting in http://localhost/ignored in the Redirect URL
  4. Generating the secret (note, you need to copy it at this point, you can't view it afterwards)
  5. Copying the secret and application ID (e.g. 4108 is an application ID) into you properties file

 The Mendeley API uses OAUTH2 which normally redirects the user to a page to confirm details.  We used anonomous access
 as described in [this stack overflow](http://stackoverflow.com/questions/23545198/oauth-2-in-mendeley-with-java) and 
 the Apache Oltu library.  This is why http://localhost/ignored was used as the application redirect (we don't use it).
 
To run this and index into ES, assuming ES is running on localhost and answering http://localhost:9200/:

 
 ```
   // edit configuration to set cluster_name to your clustername listed on http://localhost:9200/
   $ mvn package
   $ java -jar target/content-crawler.jar con-crawl --conf configuration.yml
 ```
 
 a successful run will look like:
 
 ```
 INFO  [2017-03-01 16:41:43,996+0100] [main] org.gbif.mendeley.crawl.MendeleyDocumentCrawler: Requesting data from https://api.mendeley.com/documents?group_id=dcb8ff61-dbc0-3519-af76-2072f22bc22f&limit=500
 INFO  [2017-03-01 16:41:44,497+0100] [main] org.gbif.mendeley.crawl.MendeleyDocumentCrawler: Mendeley reports total results: 4330
 INFO  [2017-03-01 16:41:44,989+0100] [main] org.gbif.mendeley.crawl.ElasticSearchIndexHandler: Indexed [500] documents
 INFO  [2017-03-01 16:41:44,990+0100] [main] org.gbif.mendeley.crawl.MendeleyDocumentCrawler: Requesting data from https://api.mendeley.com/documents?group_id=dcb8ff61-dbc0-3519-af76-2072f22bc22f&marker=d0f140bf-5b19-37e9-b9d6-84865e438eed&limit=500&reverse=false&order=asc
 INFO  [2017-03-01 16:41:45,627+0100] [main] org.gbif.mendeley.crawl.ElasticSearchIndexHandler: Indexed [500] documents
 INFO  [2017-03-01 16:41:45,627+0100] [main] org.gbif.mendeley.crawl.MendeleyDocumentCrawler: Requesting data from https://api.mendeley.com/documents?group_id=dcb8ff61-dbc0-3519-af76-2072f22bc22f&marker=04c9ab27-26d3-33fb-9e8e-d001ca9b4021&limit=500&reverse=false&order=asc
 INFO  [2017-03-01 16:41:46,219+0100] [main] org.gbif.mendeley.crawl.ElasticSearchIndexHandler: Indexed [500] documents
 INFO  [2017-03-01 16:41:46,219+0100] [main] org.gbif.mendeley.crawl.MendeleyDocumentCrawler: Requesting data from https://api.mendeley.com/documents?group_id=dcb8ff61-dbc0-3519-af76-2072f22bc22f&marker=dab2d8a4-1840-387c-88e5-b5639c0caf99&limit=500&reverse=false&order=asc
 INFO  [2017-03-01 16:41:46,686+0100] [main] org.gbif.mendeley.crawl.ElasticSearchIndexHandler: Indexed [500] documents
 INFO  [2017-03-01 16:41:46,687+0100] [main] org.gbif.mendeley.crawl.MendeleyDocumentCrawler: Requesting data from https://api.mendeley.com/documents?group_id=dcb8ff61-dbc0-3519-af76-2072f22bc22f&marker=fa9ee2cb-635f-3adf-995d-a8f8354f73e9&limit=500&reverse=false&order=asc
 INFO  [2017-03-01 16:41:47,123+0100] [main] org.gbif.mendeley.crawl.ElasticSearchIndexHandler: Indexed [500] documents
 INFO  [2017-03-01 16:41:47,124+0100] [main] org.gbif.mendeley.crawl.MendeleyDocumentCrawler: Requesting data from https://api.mendeley.com/documents?group_id=dcb8ff61-dbc0-3519-af76-2072f22bc22f&marker=fd124e8d-219c-38eb-9d46-dee67964b555&limit=500&reverse=false&order=asc
 INFO  [2017-03-01 16:41:47,832+0100] [main] org.gbif.mendeley.crawl.ElasticSearchIndexHandler: Indexed [500] documents
 INFO  [2017-03-01 16:41:47,832+0100] [main] org.gbif.mendeley.crawl.MendeleyDocumentCrawler: Requesting data from https://api.mendeley.com/documents?group_id=dcb8ff61-dbc0-3519-af76-2072f22bc22f&marker=2cbd9481-fbeb-3f40-ae3b-bbb41b2aaa7c&limit=500&reverse=false&order=asc
 INFO  [2017-03-01 16:41:48,454+0100] [main] org.gbif.mendeley.crawl.ElasticSearchIndexHandler: Indexed [500] documents
 INFO  [2017-03-01 16:41:48,454+0100] [main] org.gbif.mendeley.crawl.MendeleyDocumentCrawler: Requesting data from https://api.mendeley.com/documents?group_id=dcb8ff61-dbc0-3519-af76-2072f22bc22f&marker=ac4b864f-c540-36cd-a0f3-dc38cb1efee5&limit=500&reverse=false&order=asc
 INFO  [2017-03-01 16:41:49,039+0100] [main] org.gbif.mendeley.crawl.ElasticSearchIndexHandler: Indexed [500] documents
 INFO  [2017-03-01 16:41:49,039+0100] [main] org.gbif.mendeley.crawl.MendeleyDocumentCrawler: Requesting data from https://api.mendeley.com/documents?group_id=dcb8ff61-dbc0-3519-af76-2072f22bc22f&marker=a7d72802-57b3-30e3-9596-a7512db65edd&limit=500&reverse=false&order=asc
 INFO  [2017-03-01 16:41:49,597+0100] [main] org.gbif.mendeley.crawl.ElasticSearchIndexHandler: Indexed [330] documents 
 ```
 
 Which can be queried e.g. `$ curl -XPOST "http://localhost:9200/mendeley4/_search?pretty=true" -d '{"query" : { "query_string" : {"query" : "*"} }}'`

    
### Contentful Crawl

The Contentful API requires an authentication token and space id than 
