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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Following Data Artisan Flink Training:
 * http://dataartisans.github.io/flink-training/exercises/mailData.html
 *
 * @author Keira Zhou
 * @date 12/24/2015
 */
public class FlinkMail {

	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		// read sender and body fields
		DataSet<Tuple2<String, String>> timestampSender = env
				.readCsvFile("dataset/flinkMails.gz").lineDelimiter("##//##")
				.fieldDelimiter("#|#").includeFields("011")
				.types(String.class, String.class);

		// get the month and the sender email
		DataSet<Tuple3<String, String, Integer>> output = timestampSender
				.map(new MailProcessor()) // map (month, sender) to (month,
											// sender, 1)
				.groupBy(0, 1) // group by (month, sender)
				.reduce(new MailCounter()); // aggregate (month, sender)

		// print the result
		output.print();

		// data sink

		// execute program
		// env.execute("Flink Java Mail Count");
	}

	public static final class MailProcessor
			implements
			MapFunction<Tuple2<String, String>, Tuple3<String, String, Integer>> {

		@Override
		public Tuple3<String, String, Integer> map(Tuple2<String, String> arg0)
				throws Exception {

			// get the year and month
			String[] timeStampLine = arg0.f0.split("-");
			String timeStamp = timeStampLine[0] + "-" + timeStampLine[1];

			// get the senders' emails
			String[] senderLine = arg0.f1.split(" ");
			String sender = senderLine[senderLine.length - 1].replaceAll(
					"[><]", "");

			// map (month, sender) to (month, sender, 1)
			Tuple3<String, String, Integer> output = new Tuple3<String, String, Integer>(
					timeStamp, sender, 1);

			return output;
		}
	}

	public static final class MailCounter implements
			ReduceFunction<Tuple3<String, String, Integer>> {

		@Override
		public Tuple3<String, String, Integer> reduce(
				Tuple3<String, String, Integer> arg0,
				Tuple3<String, String, Integer> arg1) throws Exception {

			// aggregate (month, sender)
			return new Tuple3<String, String, Integer>(arg0.f0, arg0.f1,
					arg0.f2 + arg1.f2);
		}

	}
}
