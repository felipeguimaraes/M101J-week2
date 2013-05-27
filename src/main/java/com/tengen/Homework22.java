package com.tengen;

import java.net.UnknownHostException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class Homework22 {

	public static void main(String[] args) throws UnknownHostException {
		MongoClient client = new MongoClient();
		DB db = client.getDB("students");

		DBCollection collection = db.getCollection("grades");

		DBCursor cursor = collection
				.find(new BasicDBObject("type", "homework")).sort(
						new BasicDBObject("student_id", 1).append("score", 1));

		try {
			int actualStudentId = -1;
			int count = 0;
			while (cursor.hasNext()) {
				DBObject dbObject = cursor.next();
				int studentId = (Integer) dbObject.get("student_id");

				System.out.println(dbObject);

				if (actualStudentId != studentId) {
					System.out.println("removed - "
							+ dbObject.get("student_id") + " - "
							+ dbObject.get("score"));
					collection.remove(dbObject);
					count++;
				}

				actualStudentId = studentId;
			}

			System.out.println(count);

		} finally {
			cursor.close();
		}
	}
}
