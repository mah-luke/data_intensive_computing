---
title: "Report for Assignment 2"
subtitle: "Data-Intensive Computing SS2024"
author:
- Lukas Mahler (11908553)
- Julian Flür (11807481)
toc: True
---

\newpage

# Introduction
In this report we present our solution to the second assignment of the course Data-Intensive Computing,
which contained 3 main parts: 1. Redo the first assignment using RDDs, 2. Create a vectorized TF-IDF pipeline
using Spark ML and 3. Implement a SVM classifier based on the pipeline of task 2.

# Problem Overview

## Data set

We are again working on a data set of Amazon reviews.
While there are 10 attributes, such as **helpful** or a **reviewTime**, we are only interested in two of them.
Namely:

- **category**: the category that the product belongs to
- **reviewText**: the content of the review; this is the text to be processed

Each review is in exactly one category and requires no preprocessing.

The review text on the other hand has to be split into unigrams where each token is one word, those
 tokens are used to calculate the TF-IDF for each review.
We split on whitespaces as well as a list of special characters and digits.
Also all the text is cast to lower case and certain stopwords are ignored for the analysis.

## Chi-square value

The chi-square value is a metric, expressing the dependence between a token and a category.
Essentially the more often a term is used in a category and the less it is used in reviews from other categories the more important it is.

$$\chi^2_{\text{tc}}=\frac{N(AD-BC)^2}{(A+B)(A+C)(B+D)(C+D)}$$

# Methodology and Approach



## Task 1: RDDs


## Task 2: Spark ML TF-IDF pipeline



## Task 3: SVM Classifier


# Conclusions


## Result



## Runtime

In the table below the runtime is given.

| task                           | runtime           |
|--------------------------------|------------------:|
| Task 1: Assignment 1 on RDDS   | 4 min 50 sec      |
| Task 2: TF-IDF pipeline        |                   |
| Task 3: SVM model              |                   |
