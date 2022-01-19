# -*- coding: utf-8 -*-
"""
Created on Sun Jan 16 01:41:20 2022

@author: Greta
"""

# Importing needed libraries
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from googletrans import Translator
translator = Translator()
# NLTK tools for text processing
import re, nltk
from nltk import word_tokenize
from nltk.corpus import stopwords
nltk.download('stopwords')
nltk.download('punkt')
from nltk import pos_tag
nltk.download('wordnet')
from nltk.corpus import wordnet
from nltk.stem import WordNetLemmatizer
wordnet_lemmatizer = WordNetLemmatizer()
from nltk.stem import PorterStemmer
ps = PorterStemmer()
from textblob import TextBlob
from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = 'all'

# Charger les données 
df_pt = pd.read_csv(r'C:\Users\Greta\Desktop\Projet Brazil\olist_order_reviews_dataset.csv',sep=',',encoding='utf-8')

#Séparer reviews et titre, supprimer titres
review_data_title = df_pt['review_comment_title']
review_data = df_pt.drop(['review_comment_title'],axis=1)

# Drop valeurs nulles
reviews = review_data.dropna(subset=['review_comment_message'])

# Reset index
review_data = review_data.reset_index(drop=True)


# Traitement des reviews sous forme de liste
criticas = list(reviews['review_comment_message'].values)


# Regex pour enlever les retours à la ligne
criticas_temp = []
criticas_pos_quebra = []
for c in criticas:
    c = re.sub(r'\n', ' ', c)
    criticas_temp.append(c)
for c in criticas_temp:
    c = re.sub(r'\r', ' ', c)
    criticas_pos_quebra.append(c)


# Regex pour enlever les liens url
criticas_pos_hyperlink = []
for c in criticas_pos_quebra:
    urls = re.findall('(http|ftp|https)://([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:/~+#-]*[\w@?^=%&/~+#-])?', c)
    if len(urls) == 0:
        pass
    else:
        for url in urls:
            for link in url:
                c = c.replace(link, '')
        c = c.replace(':', '')
        c = c.replace('/', '')
    criticas_pos_hyperlink.append(c)

#Regex pour enlever les nombres
criticas_pos_num = []
for c in criticas_pos_hyperlink:
    c = re.sub(r'\d+(?:\.\d*(?:[eE]\d+))?', 'numero', c)
    criticas_pos_num.append(c)
    
# Regex pour tout en alphanumerique
criticas_sem_alphanumerico = []
for c in criticas_pos_num:
    c = re.sub(r'R\$', ' ', c)
    c = re.sub(r'\W', ' ', c)
    criticas_sem_alphanumerico.append(c)


# Regex pour enlever les espaces en trop
criticas_sem_espacos_adicionais = []
for c in criticas_sem_alphanumerico:
    c = re.sub(r'\s+', ' ', c)
    criticas_sem_espacos_adicionais.append(c)
    
# Sauvegarde de la df traitée après regex
processed_reviews = reviews.copy()
processed_reviews['review_after_regex'] = criticas_sem_espacos_adicionais
processed_reviews = processed_reviews.iloc[:, np.r_[4, -1, 2]]
processed_reviews.head()

# Transforme les reviews en enlevant les stopwords, tout mettre en lower case
comments = []
stopwords = nltk.corpus.stopwords.words('portuguese')

#Retrait des stopword et tout en lower case
for words in processed_reviews['review_after_regex']:
    only_letters = re.sub("[^a-zA-Z]", " ",words)
    tokens = nltk.word_tokenize(only_letters) #tokenize the sentences
    lower_case = [l.lower() for l in tokens] #convert all letters to lower case
    filtered_result = list(filter(lambda l: l not in stopwords, lower_case)) #Remove stopwords from the comments
    comments.append(' '.join(filtered_result))

# Frequence des trigrams
co = CountVectorizer(ngram_range=(3,3))
counts = co.fit_transform(comments)
important_trigrams = pd.DataFrame(counts.sum(axis=0),columns=co.get_feature_names()).T.sort_values(0,ascending=False).head(200)

# Rename colonnes
important_trigrams=important_trigrams.reset_index()
important_trigrams.rename(columns={'index':'trigrams',0:'frequency'},inplace=True)

#Prochaine etape Traduction des trigrams en anglais 
important_trigrams['trigrams_traduit'] =important_trigrams['trigrams'].map(lambda x: translator.translate(x, src="pt", dest="en").text)

#  Definition des noms, verbes, adverbes
pos_dict = {'J':wordnet.ADJ, 'V':wordnet.VERB, 'N':wordnet.NOUN, 'R':wordnet.ADV}
def token_stop_pos(text):
    tags = pos_tag(word_tokenize(text))
    newlist = []
    for word, tag in tags:
        if word.lower() not in stopwords:
            newlist.append(tuple([word, pos_dict.get(tag[0])]))
    return newlist

important_trigrams['POS tagged'] = important_trigrams['trigrams_traduit'].apply(token_stop_pos)
important_trigrams.head()

# Definition de la racine du mot
def lemmatize(pos_data):
    lemma_rew = " "
    for word, pos in pos_data:
        if not pos:
            lemma = word
            lemma_rew = lemma_rew + " " + lemma
        else:
                lemma = wordnet_lemmatizer.lemmatize(word, pos=pos)
                lemma_rew = lemma_rew + " " + lemma
    return lemma_rew

important_trigrams['Lemma'] = important_trigrams['POS tagged'].apply(lemmatize)
important_trigrams.head()


from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
analyzer = SentimentIntensityAnalyzer()


# function to calculate vader sentiment
def vadersentimentanalysis(review):
    vs = analyzer.polarity_scores(review)
    return vs['compound']

important_trigrams['Vader Sentiment'] = important_trigrams['Lemma'].apply(vadersentimentanalysis)

#Fonction pour analyser les reviews : Negatif, neutre et positif
def vader_analysis(compound):
    if compound >= 0.5:
        return 'Positive'
    elif compound <= -0.3 :
        return 'Negative'
    else:
        return 'Neutral'
important_trigrams['Vader Analysis'] = important_trigrams['Vader Sentiment'].apply(vader_analysis)
important_trigrams.head()

file_name= "trigrams_200"
important_trigrams.to_csv(file_name,index=False, encoding="utf-8", sep=";")

vader_counts = important_trigrams['Vader Analysis'].value_counts()
print(vader_counts)


import matplotlib.pyplot as plt
plt.pie(vader_counts.values, labels = vader_counts.index, autopct='%1.1f%%', shadow=False)

