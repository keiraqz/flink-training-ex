package flink.training.ex;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Collections;
import java.util.HashSet;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

/**
 * Following Data Artisan Flink Training:
 * http://dataartisans.github.io/flink-training/exercises/mailData.html
 * <p>
 * tf-idf(t, d, D) = tf(t, d) * idf(t, D).
 *
 * @author Keira Zhou
 * @date 12/26/2015
 */
public class TfIdf {

	public static long TOTALMAIL;

	public final static String[] stopwordsList = { "a", "about", "above",
			"across", "after", "afterwards", "again", "against", "all",
			"almost", "alone", "along", "already", "also", "although",
			"always", "am", "among", "amongst", "amoungst", "an", "and",
			"another", "any", "anyhow", "anyone", "anything", "anyway",
			"anywhere", "are", "around", "as", "at", "back", "be", "became",
			"because", "become", "becomes", "becoming", "been", "before",
			"beforehand", "behind", "being", "below", "beside", "besides",
			"between", "beyond", "bill", "both", "bottom", "but", "by", "can",
			"cannot", "cant", "co", "con", "could", "couldnt", "de", "do",
			"done", "down", "due", "during", "each", "eg", "eight", "either",
			"else", "elsewhere", "empty", "enough", "etc", "even", "ever",
			"every", "everyone", "everything", "everywhere", "except", "few",
			"find", "for", "former", "formerly", "found", "from", "front",
			"full", "further", "get", "give", "go", "had", "has", "hasnt",
			"have", "he", "hence", "her", "here", "hereafter", "hereby",
			"herein", "hereupon", "hers", "herself", "him", "himself", "his",
			"how", "however", "ie", "if", "in", "inc", "indeed", "interest",
			"into", "is", "it", "its", "itself", "keep", "last", "latter",
			"latterly", "least", "less", "ltd", "made", "many", "may", "me",
			"meanwhile", "might", "mill", "mine", "more", "moreover", "most",
			"mostly", "move", "much", "must", "my", "myself", "name", "namely",
			"neither", "never", "nevertheless", "next", "no", "nobody", "none",
			"noone", "nor", "not", "nothing", "now", "nowhere", "of", "off",
			"often", "on", "once", "one", "only", "onto", "or", "other",
			"others", "otherwise", "our", "ours", "ourselves", "out", "over",
			"own", "part", "per", "perhaps", "please", "put", "rather", "re",
			"same", "see", "seem", "seemed", "seeming", "seems", "serious",
			"several", "she", "should", "show", "side", "since", "sincere",
			"so", "some", "somehow", "someone", "something", "sometime",
			"sometimes", "somewhere", "still", "such", "system", "take",
			"than", "that", "the", "their", "them", "themselves", "then",
			"thence", "there", "thereafter", "thereby", "therefore", "therein",
			"thereupon", "these", "they", "thickv", "thin", "this", "those",
			"though", "through", "throughout", "thru", "thus", "to", "too",
			"top", "toward", "towards", "un", "under", "until", "up", "upon",
			"us", "very", "via", "was", "we", "well", "were", "what",
			"whatever", "when", "whence", "whenever", "where", "whereafter",
			"whereas", "whereby", "wherein", "whereupon", "wherever",
			"whether", "which", "while", "whither", "who", "whoever", "whole",
			"whom", "whose", "why", "will", "with", "within", "without",
			"would", "yet", "you", "your", "yours", "yourself", "yourselves",
			"the" };

	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		// get messageId and body fields
		DataSet<Tuple2<String, String>> idBody = env
				.readCsvFile("dataset/flinkMails.gz").lineDelimiter("##//##")
				.fieldDelimiter("#|#").includeFields("10001")
				.types(String.class, String.class);

		// count total mail
		TOTALMAIL = idBody.count();

		// process mail body
		DataSet<Tuple3<String, String, Integer>> idProcessedBody = idBody
				.map(new MailProcessor()) // ignore the reply part of a mail which starts with ">"
				.flatMap(new Tokenizer()); // tokenize mails and create a flatmap of mail content

		// count TF within each mail
		DataSet<Tuple3<String, String, Integer>> termFrequency = idProcessedBody
				.groupBy(0, 1).sum(2); // group by (mailId, token) and count

		// count DF for the whole collection
		DataSet<Tuple2<String, Integer>> docFrequency = idProcessedBody
				.distinct(0,1) // get distinct word for each mail
				.map(new DFComputer()) // map (mailId, token, 1) to (token, 1)
				.groupBy(0).sum(1); // group by token and count

		// compute TF-IDF of each mail
		DataSet<Tuple4<String, String, Integer, Integer>> tfDfJoin = termFrequency
				.join(docFrequency).where(1).equalTo(0).projectFirst(0, 1, 2)
				.projectSecond(1); // (mailId, token, tf, df)

		DataSet<Tuple3<String, String, Double>> mailTfIdf = tfDfJoin
				.map(new TfIdfComputer()); // compute tf-idf
		
		mailTfIdf.print();
		
		// data sink

		// execute program
		// env.execute("Flink Java Mail Count");
	}

	/**
	 * MapFunction:
	 * <p>
	 * Process mail body and ignore the reply part of a mail which
	 * begins with ">", ">>".. etc.
	 */
	public static final class MailProcessor implements
			MapFunction<Tuple2<String, String>, Tuple2<String, String>> {

		@Override
		public Tuple2<String, String> map(Tuple2<String, String> input)
				throws Exception {

			String[] content = input.f1.trim().split(">");
			String document = content[0];

			Tuple2<String, String> mailContent = new Tuple2<String, String>(
					input.f0, document);

			return mailContent;
		}
	}

	/**
	 * FlatMapFunction:
	 * <p>
	 * tokenize mails, remove symbols, remove stopwords, convert to lower case 
	 * and create a flatmap of mail content (mailId, token, 1, )...
	 */
	public static final class Tokenizer
			implements
			FlatMapFunction<Tuple2<String, String>, Tuple3<String, String, Integer>> {

		@Override
		public void flatMap(Tuple2<String, String> input,
				Collector<Tuple3<String, String, Integer>> output)
				throws Exception {

			HashSet<String> stopwords = new HashSet<String>();
			Collections.addAll(stopwords, stopwordsList);

			// normalize and split the line
			String[] tokens = input.f1.toLowerCase()
					.replaceAll("[^A-Za-z0-9 ]", "").split("\\W+");
			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0 && !stopwords.contains(token)) {
					output.collect(new Tuple3<String, String, Integer>(
							input.f0, token, 1));
				}
			}
		}
	}
	
	/**
	 * MapFunction:
	 * <p>
	 * Map (mailId, token, 1) to (token, 1)
	 */
	public static final class DFComputer
			implements
			MapFunction<Tuple3<String, String, Integer>, Tuple2<String, Integer>> {

		@Override
		public Tuple2<String, Integer> map(Tuple3<String, String, Integer> input)
				throws Exception {
			return new Tuple2<String, Integer>(input.f1, 1);
		}
	}

	/**
	 * MapFunction:
	 * <p>
	 * Compute tf-idf = tf * totalMail / df
	 */
	public static final class TfIdfComputer
			implements
			MapFunction<Tuple4<String, String, Integer, Integer>, Tuple3<String, String, Double>> {

		@Override
		public Tuple3<String, String, Double> map(
				Tuple4<String, String, Integer, Integer> input)
				throws Exception {
			// input: (mailId, token, tf, df)
			Double tfIdf = input.f2 * 1.0 * TOTALMAIL / input.f3;
			return new Tuple3<String, String, Double>(input.f0, input.f1, tfIdf);
		}
	}
}
