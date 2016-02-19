/**
 * 
 */
package com.testData.core.DataAnalytics;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

/**
 * @author Renish
 *
 */
public class MongoUtil {

	private static final String TOKEN_NULL = "NULL";

	private static final String K_AGENT_CONTACT = "agent_contact";

	private static final String TOEKN_NOADDRESS = "noaddress";

	private static final String COLL_PASS_NET = "passInternet";
	private static final String COLL_PASS_NET_RPT = "passInternetRepeat";
	private static final String COLL_PASS_UNIQUE = "passUnique";
	private static final String COLL_PASSENGERS = "passengers";

	private static final String DB_AIRLINE = "airline";

	private static final String K_COMPANY_ADDRESS = "company_address";
	private static final String K_UNKNOWN_CONTACT = "unknown_contact";
	private static final String K_BUSINESS_CONTACT = "business_contact";
	private static final String K_TAX_NUMBER = "tax_number";
	private static final String K_ADDRESS = "address";
	private static final String K_HOTEL_HOUSE_CONTACT = "hotel_house";
	private static final String K_EMAIL = "email";
	private static final String K_PSG_SURNAME = "psg_surname";
	private static final String K_PSG_FIRST_NAME = "psg_name";
	private static final String K_CID = "CID";
	private static final String K_MOBILE = "mobile";
	private static final String K_TCKN_IDENTITY_NO = "TCKN";
	private static final String K_PSG_NO = "psg_no";
	private static final String K_PAX = "pax";
	private static final String K_POS_AGT = "pos_agt";

	private static final String V_POST_AGT_ITT_TK = "ITT-TK";

	private static final int SORT_ASCENDING = 1;

	private static MongoCollection<Document> collPass = null;
	private static MongoCollection<Document> collPassNet = null;
	private static MongoCollection<Document> collPassNetRpt = null;
	private static MongoCollection<Document> collPassUnique = null;

	private MongoCollection<Document> destColl;
	private MongoCollection<Document> srcColl;

	private boolean documentUpdated = false;

	private static int globalRepeatStrictLevel = 5; // fn + ln + tckn + mobile +
	// unknown_contact + email

	private static int insertionCount = 0;
	private static int deletionCount = 0;

	private static int uniqueNameCount = 0;
	private static int uniqueTCKNCount = 0;
	private static int uniqueMobileCount = 0;
	private static int uniqueUnknContactCount = 0;
	private static int uniqueEmailCount = 0;

	private static MongoUtil mongoUtil;

	public MongoCollection<Document> getDestColl() {
		return destColl;
	}

	public void setDestColl(MongoCollection<Document> destColl) {
		this.destColl = destColl;
	}

	public MongoCollection<Document> getSrcColl() {
		return srcColl;
	}

	public void setSrcColl(MongoCollection<Document> srcColl) {
		this.srcColl = srcColl;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {

			mongoUtil = new MongoUtil();
			// mongoUtil.globalStrictLevel = 0;
			MongoDatabase dbAirline = mongoUtil.getMongoDB(DB_AIRLINE);

			collPass = dbAirline.getCollection(COLL_PASSENGERS);
			collPassNet = dbAirline.getCollection(COLL_PASS_NET);
			collPassNetRpt = dbAirline.getCollection(COLL_PASS_NET_RPT);
			collPassUnique = dbAirline.getCollection(COLL_PASS_UNIQUE);

			// FindIterable<Document> docs = collPass.find(new Document(K_CID,
			// 3));
			// FindIterable<Document> docs = collPass.find();

			// Document sample = docs.first();

			// 1 for Ascending, -1 for Descending
			// System.out.println("=== Step 1: Clean Identity Data ===");
			// mongoUtil.cleanS0_Data(collPass, -1);
			// System.out.println("=== Step 2: Seperate Out Internet Bookings
			// ===");
			// mongoUtil.seperateInternetCustomers();
			System.out.println("=== Step 3: Unify Internet Passengers ===");
			mongoUtil.setSrcColl(collPassNet);
			mongoUtil.setDestColl(collPassUnique);
			mongoUtil.checkS1_seperateOutUniqueAndRepeatingFromData(mongoUtil.getSrcColl());

			// mongoUtil.removeDuplicatesFromData(collPass);

			// String key = K_TCKN_IDENTITY_NO;
			String key = K_CID;
			// String value = "10085705578;16118503050;";
			// String value = "34853";//32050";//34853
			int value = 32051;

			// String keyMobile = K_MOBILE;
			// String valueMobile = "905457698042;";

			// "mobile" : "905457698042;"

			// insertData(collection);
			// queryData(collection, key, value);
			// System.out.println("========================");
			// //queryAndUpdateData(collection, key, value);
			// System.out.println("========================");
			// queryData(collection, keyMobile, valueMobile);

			// updateData(collection);
			//
			// queryAndUpdateData(collection);

			/**** Done ****/
			System.out.println("Done");

		} catch (MongoException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * @param dbAirline
	 */
	private void seperateInternetCustomers() {

		FindIterable<Document> docs = collPass.find(new Document(K_POS_AGT, V_POST_AGT_ITT_TK));

		List<Document> listDocs = new ArrayList<Document>();
		for (Document doc : docs) {
			listDocs.add(doc);
		}

		if (listDocs.size() > 0) {
			collPassNet.insertMany(listDocs);
			collPass.deleteMany(new Document(K_POS_AGT, V_POST_AGT_ITT_TK));
		} else {
			System.out.println("No ITT Docs to seperate out");
		}
	}

	private void queryUpdatedData(DBCollection collection) {
		/**** Find and display ****/
		BasicDBObject searchQuery2 = new BasicDBObject().append("name", "mkyong-updated");

		DBCursor cursor2 = collection.find(searchQuery2);

		while (cursor2.hasNext()) {
			System.out.println(cursor2.next());
		}
	}

	private void updateData(DBCollection collection) {
		/**** Update ****/
		// search document where name="mkyong" and update it with new values
		BasicDBObject query = new BasicDBObject();
		query.put("name", "mkyong");

		BasicDBObject newDocument = new BasicDBObject();
		newDocument.put("name", "mkyong-updated");

		BasicDBObject updateObj = new BasicDBObject();
		updateObj.put("$set", newDocument);

		collection.update(query, updateObj);
	}

	private void queryAndUpdateData(DBCollection collection, String key, String value) {
		/**** Find and display ****/
		BasicDBObject searchQuery = new BasicDBObject();
		searchQuery.put(key, value);

		DBCursor cursor = collection.find(searchQuery);

		while (cursor.hasNext()) {

			DBObject dbObject = cursor.next();
			System.out.println("Old DB Object: " + dbObject);

			int totalPax = Integer.parseInt(dbObject.get(K_PAX).toString());
			int psgNo = Integer.parseInt(dbObject.get(K_PSG_NO).toString());
			String[] tckns = dbObject.get(K_TCKN_IDENTITY_NO).toString().split(";");

			int tcknCount = psgNo - 1; // if psgNo is: 2, then tckn count will
										// be: 1
			BasicDBObject newDocument = new BasicDBObject();
			newDocument.put(K_TCKN_IDENTITY_NO, tckns[tcknCount]);

			BasicDBObject updateObj = new BasicDBObject();
			updateObj.put("$set", newDocument);

			collection.update(searchQuery, updateObj);

			// System.out.println(cursor.next());
		}
	}

	// private static void queryAndUpdateData(MongoCollection<Document>
	// collection, Document searchDoc,
	// BasicDBObject updateObj) {
	//
	// FindIterable<Document> passengers = collection.find(searchDoc);
	//
	// for (Document passenger : passengers) {
	//
	// // System.out.println("Old DB Object: " + dbObject);
	// collection.update(searchQuery, updateObj);
	//
	// }
	// }

	private void queryData(DBCollection collection, String key, Object value) {
		/**** Find and display ****/
		BasicDBObject searchQuery = new BasicDBObject();
		searchQuery.put(key, value);

		DBCursor cursor = collection.find(searchQuery);

		while (cursor.hasNext()) {
			System.out.println(cursor.next());
		}
	}

	private void queryAndInsertData(DBCollection collection, String key, Object value) {
		/**** Find and display ****/
		BasicDBObject searchQuery = new BasicDBObject();
		searchQuery.put(key, value);

		DBCursor cursor = collection.find(searchQuery);

		while (cursor.hasNext()) {
			// move to passInternet
			System.out.println(cursor.next());
		}
	}

	private void insertData(DBCollection collection) {
		/**** Insert ****/
		// create a document to store key and value
		BasicDBObject document = new BasicDBObject();
		document.put("name", "mkyong");
		document.put("age", 30);
		document.put("createdDate", new Date());
		collection.insert(document);
	}

	private MongoDatabase getMongoDB(String dbName) {

		MongoClient mongoClient = new MongoClient("localhost", 27017);
		MongoDatabase db = mongoClient.getDatabase(dbName);
		return db;
	}

	private MongoCollection<Document> initiateMongoConnection(String dbName, String collName) {
		/**** Connect to MongoDB ****/
		// Since 2.10.0, uses MongoClient
		MongoClient mongo = new MongoClient("localhost", 27017);

		/**** Get database ****/
		// if database doesn't exists, MongoDB will create it for you
		// DB db = mongo.getDB("testdb");
		MongoDatabase db = mongo.getDatabase(dbName);
		// DB db = mongo.getDB(dbName);

		/**** Get collection / table from 'testdb' ****/
		// if collection doesn't exists, MongoDB will create it for you

		MongoCollection<Document> collection = db.getCollection(collName);
		return collection;
	}

	private void checkS1_seperateOutUniqueAndRepeatingFromData(MongoCollection<Document> collection) {

		FindIterable<Document> passengers = collection.find();
		passengers.sort(new BasicDBObject(K_PSG_FIRST_NAME, SORT_ASCENDING));
		System.out.println("SORT_ASCENDING -------- : " + SORT_ASCENDING);

		Document prevDbDoc = null;
		String prevFirstName = null;
		String prevLastName = null;

		prevDbDoc = passengers.first();

		passengers.skip(1);
		int totalNoOfPassengers = 1;

		for (Document currDbDoc : passengers) {
			totalNoOfPassengers++;
			String currFirstName = currDbDoc.get(K_PSG_FIRST_NAME).toString().trim().toLowerCase();
			String currLastName = currDbDoc.get(K_PSG_SURNAME).toString().trim().toLowerCase();

			prevFirstName = prevDbDoc.get(K_PSG_FIRST_NAME).toString().trim().toLowerCase();
			prevLastName = prevDbDoc.get(K_PSG_SURNAME).toString().trim().toLowerCase();

			if (!prevFirstName.equals(currFirstName) && !prevLastName.equals(currLastName)) {

				uniqueNameCount++;
				System.out.println("Name Unique Count:" + uniqueNameCount);
				moveToUniquePassengers(prevDbDoc, currDbDoc);

			} else if (prevFirstName.equals(currFirstName) && prevLastName.equals(currLastName)) {

				// Case 2: - 2). Filter out remaining records with combination:
				// > (First Name + Last Name) + Identity

				String prevTCKN = prevDbDoc.get(K_TCKN_IDENTITY_NO).toString().trim().toLowerCase();
				String currTCKN = currDbDoc.get(K_TCKN_IDENTITY_NO).toString().trim().toLowerCase();

				if (prevTCKN.length() > 0 || currTCKN.length() > 0) {

					if (!prevTCKN.equals(currTCKN)) {
						// trim will handle blank/empty TCKNs
						uniqueTCKNCount++;
						System.out.println("TCKN Unique Count: " + uniqueTCKNCount);
						moveToUniquePassengers(prevDbDoc, currDbDoc);

					} else if (prevTCKN.equals(currTCKN)) {
						// Duplicate Case Found - Repeating Customers
						// Add Identity Error Check
						prevDbDoc = checkS2_MobileAndRemainingParams(prevDbDoc, currDbDoc);
					}
				} else {
					prevDbDoc = checkS2_MobileAndRemainingParams(prevDbDoc, currDbDoc);
				}

			}
		}
		System.out.println("total no. of passengers: " + totalNoOfPassengers);
	}

	/**
	 * @param prevDbDoc
	 * @param currDbDoc
	 * @return
	 */
	private Document checkS2_MobileAndRemainingParams(Document prevDbDoc, Document currDbDoc) {

		int localRepeatStrictLevel = 2;
		String prevMobile = prevDbDoc.get(K_MOBILE).toString().trim().toLowerCase();
		String currMobile = currDbDoc.get(K_MOBILE).toString().trim().toLowerCase();

		if (prevMobile.length() > 0 || currMobile.length() > 0) {

			if (prevMobile.equals(currMobile)) {
				// PUT INTO REPEATING CUSTOMERS - FIRST NAME + LAST NAME + TCKN
				// + MOBILE MATCHES

				if (globalRepeatStrictLevel <= localRepeatStrictLevel) {
					moveToRepeatPassengers(prevDbDoc, currDbDoc);
				} else {
					checkS3_RemainingParams(prevDbDoc, currDbDoc, currMobile);
				}

			} else if (!prevMobile.equals(currMobile)) {
				// Agent Case: Same Identity - could be Agent's
				// Identity, Mobile No.s are different
				uniqueMobileCount++;
				System.out.println("Unique Mobile Count: " + uniqueMobileCount);
				moveToUniquePassengers(prevDbDoc, currDbDoc);
			}
		} else {
			checkS3_RemainingParams(prevDbDoc, currDbDoc, currMobile);
		}
		return prevDbDoc;
	}

	/**
	 * @param prevDbDoc
	 * @param currDbDoc
	 * @param destColl
	 * @param srcColl
	 * @return
	 */
	private Document performProperDataMovement(Document prevDbDoc, Document currDbDoc,
			MongoCollection<Document> destColl, MongoCollection<Document> srcColl) {
		try {
			destColl.insertOne(prevDbDoc);
			insertionCount++;
			System.out.println("Insertion Count: " + insertionCount);
			System.out.println("Record Inserted into Destination Collection Successfully :" + prevDbDoc);

		} catch (MongoWriteException mre) {
			// mre.printStackTrace();
			if (mre.getMessage().contains("duplicate key error")) {
				System.out.println("=== Duplicate Key Found ===");
			}
		}

		DeleteResult deletedResult = srcColl.deleteOne(prevDbDoc);
		if (deletedResult.getDeletedCount() == 1) {
			deletionCount++;
			System.out.println("Records Deleted Successfully : " + deletionCount);
		}
		prevDbDoc = currDbDoc;
		return prevDbDoc;
	}

	/**
	 * @param prevDbDoc
	 * @param currDbDoc
	 * @param currMobile
	 */
	private void checkS3_RemainingParams(Document prevDbDoc, Document currDbDoc, String currMobile) {
		// Unknown contact is similar to Mobile just without 9
		// in front
		int localRepeatStrictLevel = 3;
		String prevUnkCont = prevDbDoc.get(K_UNKNOWN_CONTACT).toString().trim().toLowerCase();
		String currUnkCont = currDbDoc.get(K_UNKNOWN_CONTACT).toString().trim().toLowerCase();

		if (currMobile.length() > 0 && (prevUnkCont.length() > 0 || currUnkCont.length() > 0)) {

			if (currMobile.contains(currUnkCont)) {
				// no need of checking the unknown Contact
				// it will be duplicate check for phone no.
				checkS4_emailInfo(prevDbDoc, currDbDoc);

			} else if (!currMobile.contains(currUnkCont)) {

				if (prevUnkCont.equals(currUnkCont)) {
					if (globalRepeatStrictLevel <= localRepeatStrictLevel) {
						moveToRepeatPassengers(prevDbDoc, currDbDoc);
					} else {
						checkS4_emailInfo(prevDbDoc, currDbDoc);
					}

				} else if (!prevUnkCont.equals(currUnkCont)) {
					// move to unique customers
					uniqueUnknContactCount++;
					System.out.println("Unique Unknown Contact: " + uniqueUnknContactCount);
					moveToUniquePassengers(prevDbDoc, currDbDoc);
				}

			}
		} else {
			checkS4_emailInfo(prevDbDoc, currDbDoc);
		}
	}

	/**
	 * @param prevDbDoc
	 * @param currDbDoc
	 */
	private void checkS4_emailInfo(Document prevDbDoc, Document currDbDoc) {
		int localRepeatStrictLevel = 4;
		// Could be Agent's Mobile
		String prevEmail = prevDbDoc.get(K_EMAIL).toString().trim().toLowerCase();
		String currEmail = currDbDoc.get(K_EMAIL).toString().trim().toLowerCase();

		if (prevEmail.length() > 0 || currEmail.length() > 0) {

			if (prevEmail.equals(currEmail)) {
				// fn + ln + tckn + mobile + unkown_contact + email matches -
				// put into repeating customers
				if (globalRepeatStrictLevel <= localRepeatStrictLevel) {
					moveToRepeatPassengers(prevDbDoc, currDbDoc);
				} else {
					// checkS6_LegalInfo(prevDbDoc, currDbDoc);
					checkS5_TaxInfo(prevDbDoc, currDbDoc);
				}

			} else if (!prevEmail.equals(currEmail)) {
				// move to unique customers
				uniqueEmailCount++;
				System.out.println("Unique Email Count: " + uniqueEmailCount);
				moveToUniquePassengers(prevDbDoc, currDbDoc);
			}
		} else {
			// checkS6_LegalInfo(prevDbDoc, currDbDoc);
			checkS5_TaxInfo(prevDbDoc, currDbDoc);
		}
	}

	/**
	 * @param prevDbDoc
	 * @param currDbDoc
	 */
	private void checkS5_TaxInfo(Document prevDbDoc, Document currDbDoc) {
		// tax_number
		int localRepeatStrictLevel = 6;
		String prevTaxNo = prevDbDoc.get(K_TAX_NUMBER).toString().trim().toLowerCase();
		String currTaxNo = currDbDoc.get(K_TAX_NUMBER).toString().trim().toLowerCase();

		if (prevTaxNo.length() > 0 || currTaxNo.length() > 0) {
			if (prevTaxNo.equals(currTaxNo)) {

				if (globalRepeatStrictLevel <= localRepeatStrictLevel) {
					// Tax No is same the most probability that he is the same
					// person
					moveToRepeatPassengers(prevDbDoc, currDbDoc);
				} else {
					// checkS7_RemainingParams(prevDbDoc, currDbDoc);
					checkS6_addressInfo(prevDbDoc, currDbDoc);
				}

			} else if (!prevTaxNo.equals(currTaxNo)) {
				// move to unique customers
				moveToUniquePassengers(prevDbDoc, currDbDoc);
			}
		} else {
			// checkS7_RemainingParams(prevDbDoc, currDbDoc);
			checkS6_addressInfo(prevDbDoc, currDbDoc);
		}
	}

	/**
	 * @param prevDbDoc
	 * @param currDbDoc
	 */
	private void moveToUniquePassengers(Document prevDbDoc, Document currDbDoc) {
		prevDbDoc = performProperDataMovement(prevDbDoc, currDbDoc, destColl, srcColl);
	}

	/**
	 * @param prevDbDoc
	 * @param currDbDoc
	 */
	private void moveToRepeatPassengers(Document prevDbDoc, Document currDbDoc) {
		performProperDataMovement(prevDbDoc, currDbDoc, collPassNetRpt, collPassNet);
	}

	/**
	 * @param prevDbDoc
	 * @param currDbDoc
	 */
	private void checkS6_addressInfo(Document prevDbDoc, Document currDbDoc) {
		int localRepeatStrictLevel = 6;
		String prevAddress = prevDbDoc.get(K_ADDRESS).toString().trim().toLowerCase();
		String currAddress = currDbDoc.get(K_ADDRESS).toString().trim().toLowerCase();

		if ((prevAddress.length() > 0 && currAddress.length() > 0)
				&& ((!prevAddress.equalsIgnoreCase(TOEKN_NOADDRESS) && !prevAddress.contains(TOKEN_NULL))
						&& (!currAddress.equalsIgnoreCase(TOEKN_NOADDRESS) && !currAddress.contains(TOKEN_NULL)))) {

			if (prevAddress.equals(currAddress)) {
				if (globalRepeatStrictLevel <= localRepeatStrictLevel) {
					moveToRepeatPassengers(prevDbDoc, currDbDoc);
				} else {
					checkS7_RemainingParams(prevDbDoc, currDbDoc);
				}
			} else if(!prevAddress.equals(currAddress)){
				moveToUniquePassengers(prevDbDoc, currDbDoc);
			}
		} else {
			checkS7_RemainingParams(prevDbDoc, currDbDoc);
		}

	}

	/**
	 * @param prevDbDoc
	 * @param currDbDoc
	 */
	private void checkS7_RemainingParams(Document prevDbDoc, Document currDbDoc) {
		// hotel_house
		int localRepeatStrictLevel = 7;
		String prevHotelContact = prevDbDoc.get(K_HOTEL_HOUSE_CONTACT).toString().trim().toLowerCase();
		String currHotelContact = currDbDoc.get(K_HOTEL_HOUSE_CONTACT).toString().trim().toLowerCase();

		if (prevHotelContact.equals(currHotelContact)) {
			if (globalRepeatStrictLevel <= localRepeatStrictLevel) {
				moveToRepeatPassengers(prevDbDoc, currDbDoc);
			} else {
				checkS8_BizDetails(prevDbDoc, currDbDoc);
			}

		} else {
			// Hotel contacts are different - could be another hotel - so not
			// unifying at this time
			checkS8_BizDetails(prevDbDoc, currDbDoc);
		}
	}

	/**
	 * @param prevDbDoc
	 * @param currDbDoc
	 */
	private void checkS8_BizDetails(Document prevDbDoc, Document currDbDoc) {
		// business_contact
		String prevBizContact = prevDbDoc.get(K_BUSINESS_CONTACT).toString().trim().toLowerCase();
		String currBizContact = currDbDoc.get(K_BUSINESS_CONTACT).toString().trim().toLowerCase();

		if (prevBizContact.length() > 0 || currBizContact.length() > 0) {
			if (prevBizContact.equals(currBizContact)) {
				checkS9_CompanyAddress(prevDbDoc, currDbDoc);

			} else if (!prevBizContact.equals(currBizContact)) {
				moveToRepeatPassengers(prevDbDoc, currDbDoc);
			}

		} else {
			checkS9_CompanyAddress(prevDbDoc, currDbDoc);
		}
	}

	/**
	 * @param prevDbDoc
	 * @param currDbDoc
	 */
	private void checkS9_CompanyAddress(Document prevDbDoc, Document currDbDoc) {
		// company_address
		String prevCompanyAddress = prevDbDoc.get(K_COMPANY_ADDRESS).toString().trim().toLowerCase();
		String currCompanyAddress = currDbDoc.get(K_COMPANY_ADDRESS).toString().trim().toLowerCase();

		if (prevCompanyAddress.length() > 0 || currCompanyAddress.length() > 0) {
			if (prevCompanyAddress.equals(currCompanyAddress)) {
				moveToRepeatPassengers(prevDbDoc, currDbDoc);
			}
		}
	}

	public void cleanS0_Data(MongoCollection<Document> collection, int sortOrder) throws UnknownHostException {

		FindIterable<Document> passengers = collection.find();

		// sorting the cursor based in descending order based on
		// K_TCKN_IDENTITY_NO field
		passengers.sort(new BasicDBObject(K_TCKN_IDENTITY_NO, sortOrder));

		System.out.println("Sorts in Descending order-------------------------------------------");

		String prevTCKNString = "";
		String[] prevTokens = null;

		for (Document passenger : passengers) {

			documentUpdated = false;

			Document docToUpdate = cleanS1_Data(passenger);

			int totalPax = Integer.parseInt(passenger.get(K_PAX).toString());
			int psgNo = Integer.parseInt(passenger.get(K_PSG_NO).toString());
			int cID = Integer.parseInt(passenger.get(K_CID).toString());

			if (totalPax > 1) {

				System.out.println("Total No. of Passengers in this transaction: " + totalPax);
				System.out.println("Old DB Object: " + passenger);
				String tcknString = passenger.get(K_TCKN_IDENTITY_NO).toString();
				System.out.println(K_TCKN_IDENTITY_NO + ": " + tcknString);

				String[] tckns = null;
				if (tcknString.equals(prevTCKNString) && prevTokens != null && psgNo <= totalPax) {
					updatePassengersIdentityData(collection, prevTokens, psgNo, cID, docToUpdate);

				} else if (tcknString.trim().length() > 1) {

					tckns = tcknString.split(";");
					if (tckns.length == totalPax && psgNo <= totalPax) {
						prevTokens = tckns;
						prevTCKNString = tcknString;
						updatePassengersIdentityData(collection, tckns, psgNo, cID, docToUpdate);
					}
				}
			}
			// if(docToUpdate.isEmpty()){
			// System.out.println("Perfect Doc - Nothing to Update");
			// System.out.println(""+passenger);
			// }
			if (!documentUpdated && !docToUpdate.isEmpty()) {
				updatePassengersData(collection, cID, docToUpdate);
			}

		}

	}

	/**
	 * @param passenger
	 * @return Document
	 */
	private Document cleanS1_Data(Document passenger) {

		List<String> keysToClean = addKeysToCleanData();

		Document newDocument = new Document();

		for (String key : keysToClean) {

			String vParam = passenger.get(key).toString().trim();
			String modifiedvParam = removeLastCharFromString(vParam);
			if (!modifiedvParam.equalsIgnoreCase(vParam)) {
				newDocument.put(key, modifiedvParam);
			}
		}

		cleanContactInfo(passenger, newDocument);
		return newDocument;
	}

	/**
	 * @param passenger
	 * @param newDocument
	 */
	private void cleanContactInfo(Document passenger, Document newDocument) {

		String vMobile = passenger.get(K_MOBILE).toString().trim();
		String modifiedvMobile = removeLastCharFromString(vMobile);
		if (!modifiedvMobile.equals(vMobile)) {
			newDocument.put(K_MOBILE, modifiedvMobile);
		}

		boolean isUnkContUpdated = false;

		String vUnkCont = passenger.get(K_UNKNOWN_CONTACT).toString().trim();
		String modifiedvUnkCont = removeLastCharFromString(vUnkCont);

		if (vMobile.length() > 0 && modifiedvUnkCont.length() > 0) {
			if (vMobile.contains(modifiedvUnkCont)) {
				modifiedvUnkCont = "";
				newDocument.put(K_UNKNOWN_CONTACT, modifiedvUnkCont);
				isUnkContUpdated = true;
			}
		}

		if (!isUnkContUpdated && !vUnkCont.equals(modifiedvUnkCont)) {
			System.out.println("Modified Uknowon Contact: " + modifiedvUnkCont);
			newDocument.put(K_UNKNOWN_CONTACT, modifiedvUnkCont);
		}
	}

	/**
	 * @return
	 */
	private List<String> addKeysToCleanData() {

		List<String> keysToClean = new ArrayList<String>();

		keysToClean.add(K_TCKN_IDENTITY_NO);
		// keysToClean.add(K_MOBILE);
		keysToClean.add(K_ADDRESS);
		keysToClean.add(K_EMAIL);
		keysToClean.add(K_HOTEL_HOUSE_CONTACT);
		keysToClean.add(K_COMPANY_ADDRESS);
		keysToClean.add(K_TAX_NUMBER);
		keysToClean.add(K_AGENT_CONTACT);
		keysToClean.add(K_BUSINESS_CONTACT);
		// keysToClean.add(K_UNKNOWN_CONTACT);

		return keysToClean;
	}

	/**
	 * @param str
	 * @return
	 */
	private String removeLastCharFromString(String str) {

		int strLength = str.length();

		if (strLength > 0 && str.charAt(strLength - 1) == ';') {
			System.out.println("Original String :" + str);
			str = str.substring(0, strLength - 1);
			System.out.println("Modified String :" + str);
		}
		return str;
	}

	private void updatePassengersIdentityData(MongoCollection<Document> collection, String[] tckns, int psgNo, int cID,
			Document docToUpdate) {

		System.out.println("Passenger No: " + psgNo);
		int tcknCount = psgNo - 1; // if psgNo is: 2, then tckn count will be: 1

		docToUpdate.put(K_TCKN_IDENTITY_NO, tckns[tcknCount]);

		searchAndUpdateDoc(collection, cID, docToUpdate);

	}

	private void updatePassengersData(MongoCollection<Document> collection, int cID, Document docToUpdate) {
		searchAndUpdateDoc(collection, cID, docToUpdate);
	}

	/**
	 * @param collection
	 * @param cID
	 * @param docToUpdate
	 */
	private void searchAndUpdateDoc(MongoCollection<Document> collection, int cID, Document docToUpdate) {

		Document searchDoc = new Document();
		searchDoc.put(K_CID, cID);

		Document updateDoc = new Document();
		updateDoc.put("$set", docToUpdate);

		UpdateResult updatedResult = collection.updateOne(searchDoc, updateDoc);
		if (updatedResult.getModifiedCount() > 0) {
			documentUpdated = true;
			System.out.println("Update Successfully CID: " + cID);
		}
	}

}
