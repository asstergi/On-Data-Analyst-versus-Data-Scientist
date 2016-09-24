from __future__ import print_function
 
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

import math
import string

import nltk
nltk.download('punkt')
from nltk.tokenize import wordpunct_tokenize, sent_tokenize

def parseDatafileLine(text):
    """ 
    Parse a line of the data file by tokenizing each sentence and creating unigrams and bigrams exluding stopwords
    """
    
    sentences = sent_tokenize(text.lower())
    tokenized = [[i for i in wordpunct_tokenize(sentence) if i not in string.punctuation] for sentence in sentences]
    unigrams = [i for sentence in tokenized for i in sentence if i not in stop_words.value and len(i)>1]
    
    #bigrams = (['_'.join(i) for sentence in sentences for i in zip(tokenized, tokenized[1:])])
    bigrams = [a+'_'+b for sentence in tokenized for a, b in zip(sentence, sentence[1:]) if a not in stop_words.value and b not in stop_words.value and len(a)>1 and len(b)>1]
    
    unigrams.extend(bigrams)
    unigramsAndBigrams = list(set(unigrams))
    
    return unigramsAndBigrams

def splitDatafileLine(datafileLine):
    """ 
    Emit (word, 1) for each unigram of bigram in the input text
    """
    words = [(word, 1) for word in datafileLine]
    return words
    
def filterChosenWords(word, selectedWord):
    """ 
    Filter words that are contained in the selectedWord
    For example, if selectedWord = 'data_analyst', filter out 'data' and 'analyst' and 'data_analyst'
    """
    if '_' in selectedWord:
        splittedWord = selectedWord.split('_')
        return word!=splittedWord[0] and word!=splittedWord[1] and word!=selectedWord
    else:
        return word!=selectedWord
  
def returnRow(x):
    try:
        return Row(x[0], x[1][0], x[1][1])
    except:
        pass

if __name__ == "__main__":

    sc = SparkContext(appName="Words-PMI")
    sqlContext = SQLContext(sc)
    
    file = 'all_reddit_data.csv'
    
    #List of stopwords are broadcasted to the worker processes
    stop_words_list = [u'all', u'just', u'being', u'over', u'both', u'through', u'yourselves', u'its', u'before', u'herself', u'had', u'should', u'to', u'only', u'under', u'ours', u'has', u'do', u'them', u'his', u'very', u'they', u'not', u'during', u'now', u'him', u'nor', u'did', u'this', u'she', u'each', u'further', u'where', u'few', u'because', u'doing', u'some', u'are', u'our', u'ourselves', u'out', u'what', u'for', u'while', u'does', u'above', u'between', u't', u'be', u'we', u'who', u'were', u'here', u'hers', u'by', u'on', u'about', u'of', u'against', u's', u'or', u'own', u'into', u'yourself', u'down', u'your', u'from', u'her', u'their', u'there', u'been', u'whom', u'too', u'themselves', u'was', u'until', u'more', u'himself', u'that', u'but', u'don', u'with', u'than', u'those', u'he', u'me', u'myself', u'these', u'up', u'will', u'below', u'can', u'theirs', u'my', u'and', u'then', u'is', u'am', u'it', u'an', u'as', u'itself', u'at', u'have', u'in', u'any', u'if', u'again', u'no', u'when', u'same', u'how', u'other', u'which', u'you', u'after', u'most', u'such', u'why', u'a', u'off', u'i', u'yours', u'so', u'the', u'having', u'once', '.', ',', '"', "'", '?', '!', ':', ';', '(', ')', '[', ']', '{', ',}','...','..','!!!','!!','....']
    stop_words = sc.broadcast(stop_words_list)
    
    #Parameter that defines the minimum number of input texts a unigram or bigram
    #should be present in, in order to be kept in the final result
    minText = 10
    
    #Load data
    small = sc.textFile(file, use_unicode=0).map(parseDatafileLine).cache()
    print ("Data sample...")
    print (small.take(2))
    
    #The unigrams of bigrams to run the analysis on
    words = ['data_analyst', 'data_scientist']
    
    #Count unigrams and bigrams for texts containing the 1st selected word
    withWord1 = small.filter(lambda x: words[0] in x).cache()
    word1count = withWord1.count()
    print ("Word 1 count...")
    print (word1count)
    PXGivenWord1 = (withWord1.flatMap(splitDatafileLine)
                        .filter(lambda (x,y): filterChosenWords(x, words[0]))
                        .reduceByKey(lambda a,b: a+b)
                        .mapValues(lambda x: x*1.0/word1count)
                        .cache())
    
    print ("PX Word 1...")
    print (PXGivenWord1.takeOrdered(20, lambda s: -s[1]))
    
    #Count unigrams and bigrams for texts containing the 2nd selected word
    withWord2 = small.filter(lambda x: words[1] in x).cache()
    word2count = withWord2.count()
    print ("Word 2 count...")
    print (word2count)
    PXGivenWord2 = (withWord2.flatMap(splitDatafileLine)
                        .filter(lambda (x,y): filterChosenWords(x, words[1]))
                        .reduceByKey(lambda a,b: a+b)
                        .mapValues(lambda x: x*1.0/word2count)
                        .cache())
    print ("PX Word 2...")
    print (PXGivenWord2.takeOrdered(20, lambda s: -s[1]))
    
    #Count unigrams and bigrams for all texts
    textCount = small.count()
    print ("Number of input texts...")
    print (textCount)
    PX = (small.flatMap(splitDatafileLine)
                        .filter(lambda (x,y): filterChosenWords(x, words[0]) and filterChosenWords(x, words[1]))
                        .reduceByKey(lambda a,b: a+b)
                        .mapValues(lambda x: x*1.0/textCount)
                        .cache())
    
    print (PX.takeOrdered(20, lambda s: -s[1]))
    
    
    PMIWord1 = PXGivenWord1.join(PX).filter(lambda (a,b): b[1] * textCount>=minText).map(lambda (a,b): (a, math.log(b[0],2) - math.log(b[1],2)))
    print ("PMIs for " + str(words[0]))
    print (PMIWord1.takeOrdered(20, lambda s: -s[1]))
    
    PMIWord2 = PXGivenWord2.join(PX).filter(lambda (a,b): b[1] * textCount>=minText).map(lambda (a,b): (a, math.log(b[0],2) - math.log(b[1],2)))
    print ("PMIs for " + str(words[1]))
    print (PMIWord2.takeOrdered(20, lambda s: -s[1]))
    
    commonWords = PMIWord1.join(PMIWord2)
    print ("Words with relatively high PMI for both " + words[0] + " and " + words[1] +  "...")
    print (commonWords.takeOrdered(20, lambda (a,b): -(b[0] + b[1])))

    #Convert to Pandas Dataframe and save as CSV   
    commonWordsSparkDF = commonWords.map(returnRow).toDF()
    commonWordsPandasDF = commonWordsSparkDF.toPandas()
    commonWordsPandasDF.columns = ['word', 'PMI for ' + words[0], 'PMI for ' + words[1]]
    file = "results/PMIs for " + words[0] + " & " + words[1] + ".csv"
    commonWordsPandasDF.to_csv(file, index=False, encoding='utf-8')