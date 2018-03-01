Project-1(TF-IDF)

Group:
Divya Marneni
Sakshi Sachdev

TF-IDF Process:
1. We calculated term frequency(number of times a term is appearing in a document/total number of terms in a document). This contributes to our term frequency
2. Next we calculated the number of documents a particular term is appearing and this contributes to the idf
3. We retrieved the total number of documents in the corpus and this becomes the CountOfDocs
4. In the final step we combine tf and idf in the following manner.

TFIDF of a term = (tf) * log(CountOfDocs/idf)

Implementation

1.For implementing this process we wrote the termfrequencyDriver in that we initialized  three jobs:
  	a.TermfrequencyMapReduce job(job_tf):It gives term frequency (doc_id-term:tf)
  	b.IdfMapReduceJob(job_idf):It gives number of documents a term is apprearing in(term-docId : termcount-doc_count)
  	c.TF-IDFMapReduceJob(job_TF-IDF):we are retrieving total number of documents in the entire corpus.
	d. In the next step we are calculating  retrieving tf from pervious job and calculating idf using the formula (log10(total number of documents/doc_count)) 
  	d.Final Output: term-doc_id  tf-idf

Input Folder:
AfterProcessing

OutPut File;
TF_IDF
<term-docId   TFIDF>
 