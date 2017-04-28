# Spark
Summary for Learning Apache Spark 

Big Data Computing Homework

Input. 

The input text is an ebook from The Project Gutenberg, titled ”Plutarch: Lives of the noble Grecians and Romans by Plutarch” which is available at the URL:

https://www.gutenberg.org/ebooks/674.txt.utf-8

Output.

a) For each word in the input text, you must compute the occurrences of other words appear in every line that it appeared. For example, assume that we have the following line as an input text.
“Theseus seemed to me to resemble Romulus in many particulars.”
With the word “Theseus”: “to” occurs twice, “seemed” occurs once, “me” occurs once and so on.
With the word “to”, “Theseus” occurs once, “to” occurs once (not zero times!), and so on.
For this assignment, you are going to compute the occurrences of every word for all lines, not just for a single line.

b) Produce a list of words, with the top-10 words that appear with this word for all lines.

c) For each word in the input text, you must compute the occurrences of other words that appear in every line that it appeared, right after the appearance of the word. For example, assume that we have the following line as an input text
“Theseus seemed to me to resemble Romulus in many particulars.”
With the word “Theseus”, “seemed” occurs once. With the word “seemed”, “to” occurs once.
Produce a list of words, with the top-10 words that appear right after this word for all lines.

d) Generalize step (c) so that you include all words that appear up to distance k after a given word.

Guidelines :
1. Convert every string to lower-case
2. Remove any punctuation, i.e., any character other than [a...z]
3. For each sub-question submit 2 files: text file with the results (any format you believe is more suitable) and the python code.
