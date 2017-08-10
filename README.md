# HtmlEntityExtraction
It is a spark machine learning project to extract patterns in XML/Html web files

# This project is desinged to extract entities from ecommerce website, like; the true product price.
The project is in production of 7 TB trafic/hour.


#How to use the project
1. First of all you should create models on you learning set, you can find models in domain.models package; GBTDomainSuperParSelectCandid-
implements Gradient Boosted Trees, in models you have; LDA and PCA - to reduce feature vectores, Random Forest, SVM and Trees4Grams.

2. Ones the models are generated they are deploy to spark streaming app, that is consuming from Kafka the raw data and pushing 
back enriched messages with the right price, after the prediction is done. You can find all the details in streaming package, all the
streaming jobs such as; 
   FillSeedsByProdFreq - generate seeds for scraper so it can start testing here you put list of urls you want to 
scarape, this job will push them and you can use https://github.com/2dmitrypavlov/scraper project to scrape data from various website
using destributed scraper.

3. After models has finished you have lots of utilities to generate graphs out of the data by reading and writing to Cassandra tables etc.
