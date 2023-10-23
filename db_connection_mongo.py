#-------------------------------------------------------------------------
# AUTHOR: your name
# FILENAME: title of the source file
# SPECIFICATION: description of the program
# FOR: CS 4250- Assignment #2
# TIME SPENT: how long it took you to complete the assignment
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here
from pymongo import MongoClient
import datetime
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
    print(termObjects)

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
    pipeline = [
        {
            "$lookup": {
                "from": "documents",  # The collection you want to join with
                "localField": "term",  # The local field in the "term" collection
                "foreignField": "term",  # The foreign field in the "index" collection
                "as": "term_docs"  # The alias for the joined data
            }
        },
        {
            "$unwind": "$term_docs"  # Unwind the array created by the lookup
        },
        {
            "$group": {
                "_id": "$term",  # Group by the term
                "docCount": {
                    "$addToSet": "$term_docs.title"  # Create an array of unique document titles
                }
            }
        },
    ]
    result = col.aggregate(pipeline)
    # termList = {}
    for item in result:
        print(item)
        print ("divider")
