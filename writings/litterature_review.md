---
title: "Proposal: COVID and right wing parties"
output: pdf_document
author: Olivier R. Philippe
geometry: margin=2.5cm
papersize: a4
csl: apa-6th-edition.csl

---

# Introduction

The COVID-19 crisis is not the first epidemic that appears while the social




The present article focuses on the discussions on the COVID-19 by the Spanish Far right.
Previous research have shown that social media is heavily used to share information about health [REF] in general. and information in time of crisis and epidemia [REF].
Misinformation is also shared on social media and when epidemic happens, the fast news can lead to an increase of false information lie during the Zika epidemic in 2015, and the H1N1 outbreak in XXXX, the ebola in XXXX or the XXX.
The share of misinformation is increase when the epidemic is new and the scientific knowledge lacks consistency due to the constant discoveries. It was the case for the Zika virus @L49UKNTT#Bode_Vraga_2018_See.


According to @U3EFGRZB#Wang_Etal_2019_Systematic, the most studied topics on misinformation, health and social media are involving misinformation relate to vaccination, Ebola and Zika Virus.



Twitter is often use as an alternative media for health information. It provides an information exchange for users, where the information provided by news paper has a higher impact than information shared by health organisation @8DDKQDDC#Radzikowski_Etal_2016_The.
<!-- Note: Important in the current case, as the gov do lot of communications. It is possible to check that with the URLs to see if we see the same. If most of the tweets and retweets are from News orga or gov of health organisation. -->


The social media are prolific place for such spread of false news as the verification is not often done by the users and lies and false information spread faster @7Z76EIHE#Vosoughi_Roy_Aral_2018_The.


There is already complete descriptive publication on the discussion on Twitter about the COVID-19 and the relation with misinformation @42SQKUGA#Singh_Etal_2020_First.
It shows that


However, two trends also appears on social media, the rise of populism [REF], and the spread of fake news [REF].

These two phenomenons are often exacerbated in time of uncertainty such as during an epidemic [REF], or global catastrophes [REF].
It is important, therefore, to examine which type of information and which type of communication traditional populist parties are exchanging during the COVID-19 crisis.


This work focused on Twitter.

The data collection was...

Once the data collected, it needs a preprocessing steps before being usable.


## Sample and Data


We were only interested in Twitter accounts that can be identified as member of the far right parties.

The data collection starts on XXXXXX and ended on XXXXXX and is composed of a total of XXXXX tweets.

The data were access through the REST API @4B8YMVGS#No_Author__Post from Twitter.

## Preprocessing

Before analysing data, a serie of preprocessing steps is performed.
First, the identification of the language.
That identification uses the Fasttext library from Facebook to identify the tweets that are written in Spanish.
A threshold of XXX of probability to belong to a language is applied, and only those which are above and identified as in Spanish are selected.
Second, the URLs, mentions and hashtags are extracted from the tweets to be able to perform frequency analysis on them.
Third, to be able to prepare each tweet text for the LDA analysis, these entities were removed.
Exception from the hashtags for which only the symbol # has been removed.
After removing these entities, only the tweets that have more than XXXX word in them were kept.
Any tweets that had less is likely only containing an URLs and/or mentions without added content.

The remaining dataset is prepared for further analysis, lowered case, stop word removed, tokenised and then transformed in vector.


## Analytical Approach

An unsupervised method to detect topic clustering

The exploration of the message composition with previously unknown topics can be done using inductive methods that
categorize the material according to its underlying thematic structure, for example, on the basis of a statistical analysis
of the probability, coherence, and similarity of word patterns. This class of methods, like document clustering or topic

\newpage

# References
