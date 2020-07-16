---
title: "Proposal: Automatic topic modelling of official communication from Spanish far right deputies during the COVID crisis"
output: pdf_document
author: Olivier R. Philippe
geometry: margin=2cm
papersize: a4
<!-- csl: apa-6th-edition.csl -->
csl: acm-sig-proceedings.csl
fontsize: 10pt
---
# Introduction
The present proposal focuses on the discussion by the Spanish far right elected representative during the COVID-19 crisis.
It explores the topics around which the Twitter discourse from a selected VOX members' accounts revolves around. The topics detection model will be perform using different unsupervised topic modelling methods and manual labelling of the resulting topics distribution for further analysis.

# Context
Twitter is often used as an alternative media for health information @8DDKQDDC#Radzikowski_Etal_2016_The  In case of epidemic and pandemic the uncertainty and fast news can lead to an increase of false information as the verification is not often done by the users and lies and false information spread faster than reliable ones @7Z76EIHE#Vosoughi_Roy_Aral_2018_The.

This phenomenon happened during the H1N1 outbreak in 2009   the Ebola in 2013 and the Zika epidemic in 2015  @Z3TXB2YJ#Miller_Etal_2017_What, @U3EFGRZB#Wang_Etal_2019_Systematic, @L49UKNTT#Bode_Vraga_2018_See. Naturally the COVID-19 pandemic raises the same issues @NWX482NT#Xaudiera_Cardenal_2020_Ibuprofen,@7FXJBAG8#Huang_Carley_2020_Disinformation.

There is a complete descriptive publication on the discussion on Twitter about the COVID-19 and the relation with misinformation @42SQKUGA#Singh_Etal_2020_First. However, the COVID-19 is more general than a health issues, considering the novelty, extend and impact of the confinement and health misinformation is only one aspect.
Populism and far right in particular are also analysed and their own reaction to the COVID-19 and the spreading of conspiracy theories @G79UZ78C#Mason_2020_Europes, @B3PBT7TX#Ahmed_Etal_2020_Covid19 and the increase of hate messages during this pandemic @WVSETUE7#Velásquez_Etal_2020_Hate.

In this context of rapid change and fertile context for misinformation, it is important to unveil the ensemble of topics that far right talk about, and not only about the risk posed by health misinformation. This is why topic modelling is an important tool to tackle this issue.


# Data and methods

Topic modelling has been used in the context of COVID-19 crisis.
Wicke and Bolognesi  used the latent Dirichlet allocation LDA method an their existing framework to model discourse on COVID-19 @QJ3LVAHR#Wicke_Bolognesi__Framing, Lwin et al. did a sentiment analysis on the Twitter trends @T4CTMAYQ#Lwin_Etal_2020_Global and Haman et al. on the communication  by state leaders @PVBKPYZW#Haman__The.

A contrario to existing studies using large set of available tweets about the COVID-19 @GRSADCA2#Banda_Etal_2020_Largescale, or following keywords of pre-identified issues and hashtags, our data target is a finite set of users, the entire elected deputies elected in 2019 and present on Twitter.

The data collection covers the entire period of 1<sup>st</sup> January 2020 until the 15<sup>th</sup> of July 2020, accessed using the REST API  from Twitter @4B8YMVGS#No_Author__Post.
The method used is a combination of a fine-tuned language model @4ZNQUTS9#Xie_Etal_2020_Monolingual, @AABRZUJ4#Devlin_Etal_2019_Bert on the dataset and using the created word embeddings with LDA @N6V7JPRB#Blei__Latent to model the topic distribution.
From there, it is possible to compare between different discourses made by elected officials in Spain, and unveil the difference between parties and see if the far right differs from the others parties.
<!-- However, the rapid change of COVID-19 also calls for a method to detect topics formation and disappearance overtime as the context evolves in time of crisis @FHTMMECV#Li_Etal_2018_Comparison, @4Q6ZI6MW#Liu_Etal_2019_Finding. Methods exists to apply online version of the LDA on short text @IEX8A37Y#Gao_Etal_2020_Generation, @I6PIQ3NB#Nugroho_Etal_2020_Survey, which will be apply here to see the evolution of the interest from the VOX deputies during the COVID-19 crisis.  -->

\newpage

# References
