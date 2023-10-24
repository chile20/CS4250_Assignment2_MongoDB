#-------------------------------------------------------------------------
# AUTHOR: your name
# FILENAME: index.py
# SPECIFICATION: The program includes a set of functions for interacting with a MongoDB database.
# It includes functions for establishing a database connection, creating, updating, and deleting documents,
# as well as generating an index of terms with their associated document counts.
# FOR: CS 4250- Assignment #2
# TIME SPENT: 4h
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here
from pymongo import MongoClient
import re
import string

def connectDataBase():

    # Create a database connection object using pymongo
    # --> add your Python code here
    # Creating a database connection object using psycopg2

    DB_NAME = "CPP"
    DB_HOST = "localhost"
    DB_PORT = 27017
    try:
        client = MongoClient(host=DB_HOST, port=DB_PORT)
        db = client[DB_NAME]
        return db
    except:
        print("Database not connected successfully")

def createDocument(col, docId, docText, docTitle, docDate, docCat):

    # create a dictionary to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    # --> add your Python code here
    indexList = {}
    translator = str.maketrans('', '', string.punctuation)
    termInDoc = [term.translate(translator).lower() for term in docText.split() if term]
    for term in termInDoc:
        if term in indexList:
            indexList[term] += 1
        else:
            indexList[term] = 1

    # create a list of dictionaries to include term objects.
    # --> add your Python code here
    termObjects = []
    for term, count in indexList.items():
        termObjects.append({"term": term, "count": count})

    #Producing a final document as a dictionary including all the required document fields
    # --> add your Python code here
    docText_cleaned = re.sub(r'[^\w]', '', docText)
    numChar = len(docText_cleaned)
    document = {"_id": docId,
                "text": docText,
                "title": docTitle,
                "num_chars": numChar,
                "date": docDate,
                "category": docCat,
                "term": termObjects}

    # Insert the document
    # --> add your Python code here
    col.insert_one(document)
    print("Document created")
def deleteDocument(col, docId):
    # Delete the document from the database
    # --> add your Python code here
    col.delete_one({"_id": docId})
    print("Document deleted!")
def updateDocument(col, docId, docText, docTitle, docDate, docCat):

    # Delete the document
    # --> add your Python code here
    deleteDocument(col, docId)
    # Create the document with the same id
    # --> add your Python code here
    createDocument(col, docId, docText, docTitle, docDate, docCat)
def getIndex(col):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # --> add your Python code here
    termList = {}
    # Retrieve all terms
    pipeline = [
        {
            "$unwind": "$term"
        },
        {
            "$group": {
                "_id": "$term.term"
            }
        },
        {
            "$project": {
                "_id": 0,
                "term": "$_id"
            }
        },
        {
            "$sort": {
                "term": 1  # Sort in ascending order by the "term" field
            }
        }

    ]
    result = col.aggregate(pipeline)
    for item in result:
        term = item['term']
        # Retrieve documents where the term occurs with their counts
        pipeline = [
            {
                "$unwind": "$term"
            },
            {
                "$match": {
                    "term.term": term
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "document": "$title",
                    "count": "$term.count"
                }
            },
            {
                "$sort": {
                    "document": 1  # Sort in ascending order by the "term" field
                }
            }
        ]
        term_documents = col.aggregate(pipeline)
        docCountItems = [f"{row['document']}:{row['count']}" for row in term_documents]
        docCount = ", ".join(docCountItems)
        termList[term] = docCount
    return termList