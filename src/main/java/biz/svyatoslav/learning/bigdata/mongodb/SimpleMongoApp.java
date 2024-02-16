package biz.svyatoslav.learning.bigdata.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.util.Arrays;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Updates.set;

public class SimpleMongoApp {

    public static void main(String[] args) {

        SLF4JBridgeHandler.install(); // This is just to silence log messages.

        // Make sure that MongoDB is running, check the ip address.

        String uri = "mongodb://admin:123456@172.17.0.2:27017/admin";
        MongoClient mongoClient = MongoClients.create(uri);
        MongoDatabase database = mongoClient.getDatabase("library");
        MongoCollection<Document> collection = database.getCollection("books");

        System.out.println("\nGetting and printing a specific book:");
        Document specificBook = collection.find(eq("name", "Eugene Onegin")).first();
        if (specificBook != null) {
            System.out.println(specificBook.toJson());
        } else {
            System.out.println("No matching book found for 'Eugene Onegin'.");
        }

        System.out.println("\nGetting and printing all books:");
        for (Document doc : collection.find()) {
            System.out.println(doc.toJson());
        }

        System.out.println("\nGetting and printing books published after 1990:");
        for (Document doc : collection.find(gt("year", 1990))) {
            System.out.println(doc.toJson());
        }

        System.out.println("\nGetting and printing books published after 1990 (name and publishing year only):");
        for (Document doc : collection.find(gt("year", 1990))) {
            String bookName = doc.getString("name");
            int yearPublished = doc.getInteger("year");
            System.out.println("Book: " + bookName + ", Year: " + yearPublished);
        }

        System.out.println("\nInserting a book:");
        Document newBook = new Document("_id", new ObjectId())
                .append("name", "New Book Title For Test")
                .append("year", 2024)
                .append("quantity", 3)
                .append("authors", Arrays.asList("Author One", "Author Two"))
                .append("genres", Arrays.asList("Genre One", "Genre Two"));
        collection.insertOne(newBook);
        System.out.println("Inserted a new book with ID: " + newBook.getObjectId("_id"));

        System.out.println("\nChecking the insert result:");
        Document specificBookInsertCheck = collection.find(eq("name", "New Book Title For Test")).first();
        if (specificBookInsertCheck != null) {
            System.out.println(specificBookInsertCheck.toJson());
        } else {
            System.out.println("No matching book found for 'New Book Title For Test'.");
        }

        System.out.println("\nUpdating a book:");
        Bson filterForUpdate = eq("name", "New Book Title For Test");
        Bson updateOperation = set("quantity", 1000);
        UpdateResult updateResult = collection.updateOne(filterForUpdate, updateOperation);
        System.out.println("Matched count: " + updateResult.getMatchedCount());
        System.out.println("Modified count: " + updateResult.getModifiedCount());

        System.out.println("\nChecking the update result:");
        Document specificBookUpdateCheck = collection.find(eq("name", "New Book Title For Test")).first();
        if (specificBookUpdateCheck != null) {
            System.out.println(specificBookUpdateCheck.toJson());
        } else {
            System.out.println("No matching book found for 'New Book Title For Test'.");
        }

        System.out.println("\nDeleting a book:");
        Bson filterForDelete = eq("name", "New Book Title For Test");
        DeleteResult deleteResult = collection.deleteOne(filterForDelete);
        System.out.println("Deleted count: " + deleteResult.getDeletedCount());

        System.out.println("\nChecking the delete result:");
        Document specificBookDeleteCheck = collection.find(eq("name", "New Book Title For Test")).first();
        if (specificBookDeleteCheck != null) {
            System.out.println(specificBookDeleteCheck.toJson());
        } else {
            System.out.println("No matching book found for 'New Book Title For Test'.");
        }

        mongoClient.close();

    }
}