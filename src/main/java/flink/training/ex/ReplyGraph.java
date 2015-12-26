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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * Following Data Artisan Flink Training:
 * http://dataartisans.github.io/flink-training/exercises/replyGraph.html
 *
 * @author Keira Zhou
 * @date 12/24/2015
 */
public class ReplyGraph {

	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		// read messageID, sender info, replied-to fields
		DataSet<Tuple3<String, String, String>> replyRelation = env
				.readCsvFile("dataset/flinkMails.gz").lineDelimiter("##//##")
				.fieldDelimiter("#|#").includeFields("101001")
				.types(String.class, String.class, String.class);

		// get entries that replied-to is NOT null
		// process sender field
		DataSet<Tuple3<String, String, String>> replyRelationNew = replyRelation
				.filter(new FilterFunction<Tuple3<String, String, String>>() {
					// filter out those replied-to is null
					public boolean filter(Tuple3<String, String, String> value) {
						return !value.f2.equals("null");
					}
				}).map(new MailProcessor());

		// replyRelationNew join itself where replied-to ID equal to messageID
		DataSet<Tuple2<String, String>> joinedMsg = replyRelationNew
				.join(replyRelationNew).where(2) // where replied-to ID
				.equalTo(0) // equal to messageID
				.projectFirst(1) // get email from first dataset
				.projectSecond(1); // get email from second dataset

		
		DataSet<Tuple3<String, String, Integer>> output = joinedMsg
				.filter(new FilterFunction<Tuple2<String, String>>() {
					// filter out email reply to itself
					public boolean filter(
							Tuple2<String, String> value) {
						return !value.f0.equals(value.f1);
					}
				}).groupBy(0,1) // group by sender and replied-to emails
				.reduceGroup(new ReplyCounter()); // count

		// print the result
		output.print();

		// data sink

		// execute program
		// env.execute("Flink Java Mail Count");
	}

	public static final class MailProcessor
			implements
			MapFunction<Tuple3<String, String, String>, Tuple3<String, String, String>> {

		@Override
		public Tuple3<String, String, String> map(
				Tuple3<String, String, String> arg0) throws Exception {

			// get the senders' emails
			String[] senderLine = arg0.f1.split(" ");
			String sender = senderLine[senderLine.length - 1].replaceAll(
					"[><]", "");

			Tuple3<String, String, String> output = new Tuple3<String, String, String>(
					arg0.f0, sender, arg0.f2);

			return output;
		}
	}

	public static final class ReplyCounter implements
			GroupReduceFunction<Tuple2<String, String>, Tuple3<String, String, Integer>> {

		Tuple3<String, String, Integer> outTuple = new Tuple3<>();
		
		@Override
		public void reduce(Iterable<Tuple2<String, String>> input,
				Collector<Tuple3<String, String, Integer>> output)
				throws Exception {
			outTuple.f2 = 0;

			for(Tuple2<String, String> reply : input) {
				outTuple.f0 = reply.f0;
				outTuple.f1 = reply.f1;
				outTuple.f2 += 1;
			}
			output.collect(outTuple);
		}

	}

}
