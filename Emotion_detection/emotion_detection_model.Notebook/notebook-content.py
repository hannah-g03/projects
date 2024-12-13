# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7028769d-5193-4c93-974a-a1bd0a93905e",
# META       "default_lakehouse_name": "MovieData",
# META       "default_lakehouse_workspace_id": "346e1b64-7681-4e36-aaf6-454ec69177f7"
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### <u>Notebook for the NLP emotion detection model</u>
# 
# emotion dataset used : https://www.kaggle.com/datasets/abdallahwagih/emotion-dataset?resource=download
# 
# "_This Emotion Classification dataset is designed to facilitate research and experimentation in the field of natural language processing and emotion analysis. It contains a diverse collection of text samples, each labeled with the corresponding emotion it conveys. Emotions can range from happiness and excitement to anger, sadness, and more._"
# 
# **NLP** = Natural Language Processing


# MARKDOWN ********************

# Goal = Build an AI model that can accurately identify emotions like sadness, joy, love, anger, fear, and surprise from text

# CELL ********************

import pandas as pd
import numpy as np

from datasets import load_dataset

from nltk.tokenize import word_tokenize
from nltk.corpus import wordnet
from nltk import pos_tag
from nltk.stem import WordNetLemmatizer

from sklearn.model_selection import train_test_split

from sklearn.model_selection import RandomizedSearchCV

from sklearn.linear_model import LogisticRegression ## classification
from sklearn.feature_extraction.text import CountVectorizer ## text into a matrix of numbers
from sklearn.model_selection import train_test_split 
from sklearn.metrics import accuracy_score
from sklearn.pipeline import Pipeline ## pipelines automate the machine learning workflows
from sklearn.utils.class_weight import compute_class_weight # to fix class inbalance
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import StackingClassifier # allows you to have multiple models in a pipeline
from sklearn.ensemble import VotingClassifier # allows you to have multiple models in a pipeline
from sklearn.model_selection import GridSearchCV # to find optimal hyperparameters

import warnings
warnings.filterwarnings('ignore')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load in dataset 
df = pd.read_parquet("hf://datasets/dair-ai/emotion/unsplit/train-00000-of-00001.parquet")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Data Preprocessing:**

# CELL ********************

# Change label column to corresponding emotion 

df.loc[df['label'] == 0, 'label'] = 'sadness'
df.loc[df['label'] == 1, 'label'] = 'joy'
df.loc[df['label'] == 2, 'label'] = 'love'
df.loc[df['label'] == 3, 'label'] = 'anger'
df.loc[df['label'] == 4, 'label'] = 'fear'
df.loc[df['label'] == 5, 'label'] = 'surprise'


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df['label'].value_counts()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **Remove 'stop' words** : 
# "_Stop words are a set of commonly used words in a language. Examples of stop words in English are “a,” “the,” “is,” “are,” etc. Stop words are commonly used in Text Mining and Natural Language Processing (NLP) to eliminate words that are so widely used that they carry very little useful information_"
# - Using nltk library
# - https://www.geeksforgeeks.org/removing-stop-words-nltk-python/


# CELL ********************

import nltk
from nltk.corpus import stopwords
 
nltk.download('stopwords')
stop_words = set(stopwords.words('english'))
print(stop_words)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Lambda function to remove stop words (in list above) from all entries in the text column

df['text'] = df['text'].apply(lambda x: ' '.join([word for word in x.split() if word.lower() not in stop_words]))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Tokenize** :
#  '_The process itself involves breaking a larger text body into smaller units, often down to just sentences, groups of words (often referred to as n-grams), single words, or even just groups of characters or subwords. These tokens can then be further processed and fed into different NLP processes_' 
# - https://medium.com/@kelsklane/tokenization-with-nltk-52cd7b88c7d

# CELL ********************

# tokenize (using nltk library)
nltk.download('punkt')
df['text'] = df['text'].apply(word_tokenize)
df.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **Lemmatization** : 
# ##### - technique used to reduce words to their root word
# ##### - relies on accurately determining the intended part-of-speech and the meaning of a word based on its context
# - POS tagging = Part of Speech (POS) tagging refers to assigning each word of a sentence to its part of speech. It is significant as it helps to give a better syntactic overview of a sentence. 
# ##### - https://www.datacamp.com/tutorial/stemming-lemmatization-python

# CELL ********************

nltk.download("wordnet")
nltk.download('averaged_perceptron_tagger')

# initialise
wnl = WordNetLemmatizer()

# https://stackoverflow.com/questions/15586721/wordnet-lemmatization-and-pos-tagging-in-python

# function to map NLTK POS tags to WordNet POS tags
def get_wordnet_pos(treebank_tag):
    if treebank_tag.startswith('J'):
        return wordnet.ADJ
    elif treebank_tag.startswith('V'):
        return wordnet.VERB
    elif treebank_tag.startswith('N'):
        return wordnet.NOUN
    elif treebank_tag.startswith('R'):
        return wordnet.ADV
    else:
        return wordnet.NOUN  # Default to noun if no match

# function to perform tagging and lemmatization
def lemmatize_text(text_tokens):
    pos_tags = pos_tag(text_tokens)  # POS tagging
    lemmatized_tokens = [wnl.lemmatize(word, get_wordnet_pos(tag)) for word, tag in pos_tags]
    return lemmatized_tokens

df['text'] = df['text'].apply(lemmatize_text)

df.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## converting list of tokens back to strings
df['text'] = df['text'].apply(lambda tokens: ' '.join(map(str, tokens)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### - Decided to drop some values from joy and sadness to even out the classes

# CELL ********************

# drop % from each
joy_drop = int(0.3 * df[df['label'] == 'joy'].shape[0])
sadness_drop = int(0.2 * df[df['label'] == 'sadness'].shape[0])

# randomly select index to drop for each class
joy_drop_idx = np.random.choice(df[df['label'] == 'joy'].index, joy_drop, replace=False)
sadness_drop_idx = np.random.choice(df[df['label'] == 'sadness'].index, sadness_drop, replace=False)

df = df.drop(joy_drop_idx).drop(sadness_drop_idx)

xfeatures_dropped = df['text']
ylabels_dropped = df['label']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ylabels_dropped.value_counts()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Model training and testing:**

# CELL ********************

# getting train & test splits

size = int(len(xfeatures_dropped)*0.70)
x_train_d, x_test_d = xfeatures_dropped[0:size], xfeatures_dropped[size:]

#y labels
size = int(len(ylabels_dropped)*0.70)
y_train_d, y_test_d = ylabels_dropped[0:size], ylabels_dropped[size:]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

x_train_d.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# COUNT VECTORIZER & LOGISTIC REGRESSION 

pipeline_nlp3 = Pipeline(steps=[('cv', CountVectorizer()),('lr', LogisticRegression())])

class_weights = compute_class_weight('balanced', classes=np.unique(y_train_d), y=y_train_d)
pipeline_nlp3.steps[1][1].set_params(class_weight='balanced')

# fit the model 
pipeline_nlp3.fit(x_train_d,y_train_d)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# accuracy score
pipeline_nlp3.score(x_test_d,y_test_d)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sample_text5 = "i dropped my ice cream on the floor"
pipeline_nlp3.predict([sample_text5])

# WRONG

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sample_text6 = "I had roast dinner to eat, it was really nice"
pipeline_nlp3.predict([sample_text6])

# CORRECT

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sample_text7 = "its dark outside, im frightened"
pipeline_nlp3.predict([sample_text7])

# CORRECT

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sample_text8 = "someone stole my phone!"
pipeline_nlp3.predict([sample_text8])

# CORRECT

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sample_text9 = "wow i didnt expect that today, im shocked!"
pipeline_nlp3.predict([sample_text9])

# CORRECT

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ##### - adding a random forest in 

# CELL ********************

pipeline_nlp4 = Pipeline(steps=[('cv', CountVectorizer()),('sc', StackingClassifier(estimators=[('lr', LogisticRegression(class_weight='balanced')),('rf', RandomForestClassifier(n_estimators=100, max_depth=5))]))])

# fit the model 
pipeline_nlp4.fit(x_train_d,y_train_d)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# accuracy score
pipeline_nlp4.score(x_test_d,y_test_d)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sample_text10 = "the alleyway was dark, i didnt want to go through"
pipeline_nlp4.predict([sample_text10])

# CORRECT

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sample_text11 = "my computer broke and i cant fix it"
pipeline_nlp4.predict([sample_text11])

# CORRECT

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sample_text12 = "its my birthday today yay"
pipeline_nlp4.predict([sample_text12])

# CORRECT

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sample_text12 = "someone knocked at my door, i wasnt expecting it"
pipeline_nlp4.predict([sample_text12])

# WRONG (POSSIBLY CORRECT)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### - Adding in a grid search to try and get better accuracy

# CELL ********************

### voting classifier

pipeline_nlp5 = Pipeline(steps=[('cv', CountVectorizer()),('vc', VotingClassifier(estimators=[('lr', LogisticRegression(class_weight='balanced')),('rf', RandomForestClassifier(n_estimators=100, max_depth=3))]))])

# define the hyperparameter grid
param_grid = {
    'cv__max_features': [5000, 10000, 20000],
    'vc__estimators': [
        [('lr', LogisticRegression(class_weight='balanced')), ('rf', RandomForestClassifier(n_estimators=100, max_depth=3))],
        [('lr', LogisticRegression(class_weight='balanced')), ('rf', RandomForestClassifier(n_estimators=200, max_depth=5))],
        [('lr', LogisticRegression(class_weight='balanced')), ('rf', RandomForestClassifier(n_estimators=500, max_depth=10))]
    ],
    'vc__voting': ['hard', 'soft'],
    'vc__weights': [[1, 1], [2, 1], [1, 2]]
}

# perform grid search
grid_search = GridSearchCV(pipeline_nlp5, param_grid, cv=5, scoring='accuracy')
grid_search.fit(x_train_d, y_train_d)

print("Best Parameters: ", grid_search.best_params_)
print("Best Accuracy: ", grid_search.best_score_)

# use the best model to make predictions
best_model = grid_search.best_estimator_

sample_text13 = "wow that flower is pretty"
best_model.predict([sample_text13])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pipeline_nlp6 = Pipeline(steps=[('cv', CountVectorizer(max_features=5000)),('vc', VotingClassifier(estimators=[('lr', LogisticRegression(class_weight='balanced')),('rf', RandomForestClassifier(max_depth=3))], voting='hard', weights=[2,1]))])

# fit the model 
pipeline_nlp6.fit(x_train_d,y_train_d)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# accuracy score
pipeline_nlp6.score(x_test_d,y_test_d)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sample_text14 = "I'm so scared about tomorrow's presentation, I don’t think I’m ready."
pipeline_nlp6.predict([sample_text14])

# CORRECT

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sample_text15 = "Ugh, I'm so frustrated with how slow the traffic is today."
pipeline_nlp6.predict([sample_text15])

# CORRECT

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sample_text16 = "I can't believe I won the competition; it feels surreal!"
pipeline_nlp6.predict([sample_text16])

# CORRECT

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sample_text17 = "I'm completely exhausted, but at least it's finally over"
pipeline_nlp6.predict([sample_text17])

# WRONG (POSSIBLY CORRECT)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Best accuracy = 0.9094036479586994
