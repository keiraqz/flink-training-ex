# Flink Training Exercises

My solutions to Flink Training Exercises from Data Artisan: http://dataartisans.github.io/flink-training/index.html.

All my solutions are <a href="https://github.com/keiraqz/flink-training-ex/tree/master/src/main/java/flink/training/ex" target="_blank">here</a>.

## Exercise 1: <a href="http://dataartisans.github.io/flink-training/exercises/mailCount.html" target="_blank">Mail Count</a>

- **Description**: The task of the “Mail Count” exercise is to count the number of emails in the archive of the Flink development mailing list for each unique combination of email address and month.
- **My solution**: <a href="https://github.com/keiraqz/flink-training-ex/blob/master/src/main/java/flink/training/ex/FlinkMail.java" target="_blank">FlinkMail.java</a>

## Exercise 2: <a href="http://dataartisans.github.io/flink-training/exercises/replyGraph.html" target="_blank">Reply Graph</a>

- **Description**: The task of the “Reply Graph” exercise is to extract **reply connections** between mails in the archives of Apache Flink’s developer mailing list. A **reply connection** is defined as a pair of two email addresses (Tuple2\<String, String\>) where the first email address replied to an email of the second email address. The task of this exercise is to compute all reply connections between emails in the Mail Data Set and count the number of reply connections for each unique pair of email addresses.
- **My solution**: <a href="https://github.com/keiraqz/flink-training-ex/blob/master/src/main/java/flink/training/ex/ReplyGraph.java" target="_blank">ReplyGraph.java</a>

## Exercise 3: <a href="http://dataartisans.github.io/flink-training/exercises/tfIdf.html" target="_blank">TF-IDF</a>

- **Description**: The task of the TF-IDF exercise is to compute the term-frequency/inverted-document-frequency (TF-IDF) metric for words in mails of the Flink developer mailing list archives.
- **My solution**: <a href="https://github.com/keiraqz/flink-training-ex/blob/master/src/main/java/flink/training/ex/TfIdf.java" target="_blank">TfIdf.java</a>
