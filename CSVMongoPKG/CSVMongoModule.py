import pymongo
import csv
import os
import logging

logging.basicConfig(filename="MongoDB_CSV.log",level=logging.INFO,format="%(asctime)s - %(name)s - %(levelname)s - %(msg)s")

class CsvtoDb:
    def __init__(self):
        while True:
            try:
                self.filename = input("Enter the csv file path to load to MongoDB :")
                logging.info(f"Filename provided as input, filename is {self.filename}")
                self.isCsvfile()
                logging.info(f"{self.filename} is a csv file ")
                break
            except Exception as e:
                logging.info(str(e))
                logging.info("Asking user to enter a valid filepath")
                continue

    def establish_connection(self):
        """
        This method establishes connection between MongoDB server and python client

        :return: MongoClient connection object
        """
        while True:
            try:
                self.client = pymongo.MongoClient(input("Please Enter you Mongo DB cluster connection string : "))

                if self.client.list_database_names():
                    logging.info("valid Connection string entered !!")
                    return self.client
                else:
                    logging.error("Invalid Connection string entered !!")
                    continue
            except Exception as e:
                logging.error("ERROR !!" + str(e))

    def create_collection(self):
        """
        This method creates new DB and collection/ connects to an existing DB and collection by
        taking user inputs for collection_name and database.

        """
        try:
            self.collection_name = input("Please enter the collection Name : ")
            self.database = self.client[input("Please enter the Database Name : ")]
            self.collection = self.database[self.collection_name]
            logging.info(f"Collection Name : {self.collection_name} \n Database Name : {self.database}")
        except Exception as e:
            logging.error("ERROR !!" + str(e))

    def isCsvfile(self):
        """
        Checks if the file provided is a csv file.

        :return: True if it's csv file, else raises an exception
        """
        if self.filename.endswith(".csv") and os.path.isfile(self.filename):
            return 1
        raise Exception(self.filename, " is not a valid csv file")

    def inserttoDb(self):
        try:
            logging.info(f"Insert to DB operation started on {self.filename}")
            with open(self.filename, "r") as f:
                logging.info(f"{self.filename} opened for read and write to DB operation ")
                data_csv = csv.reader(f, delimiter="\n")
                heading = next(data_csv)
                # print(heading)
                heading_keys = heading[0].split(";")
                # print(heading_keys)

                d_count = 0
                for row in data_csv:
                    d_count += 1
                    record = {}
                    values_lst = row[0].split(";")

                    for key in heading_keys:
                        for value in values_lst:
                            record[key] = value
                            values_lst.remove(value)
                            break

                    # One record/document generated, now insert

                    self.collection.insert_one(record)
                logging.info(f"{d_count} documents inserted into {self.collection_name} collection ")
        except Exception as e:
            logging.error("ERROR !!" + str(e))

    def update_documents(self, present_data, new_data):
        """
        Updates the documents with the new_data where present_data is found.

        :param present_data: key, value pair from the collection
        :param new_data: key, value pair of new data

        """
        try:
            self.collection.update_many(present_data, new_data)
            logging.info(f"{self.collection_name} updated wherever {present_data} match was found. ")
        except Exception as e:
            logging.error("ERROR!!" + str(e))

    def find_document(self, query):
        """
        Query the database

        :param query: key, value pair to match
        :return: cursor object to iterate over
        """
        try:
            logging.info(f"Querying DB for {query} matched documents ")
            return self.collection.find(query)
        except Exception as e:
            logging.error("ERROR !!" + str(e))

    def delete_document(self, query_to_delete):
        """
        deletes documents from a collection wherever query_to_delete match is found

        :param query_to_delete: key, value pair to match
        """
        try:
            logging.info(f"Deleting documents in {self.collection_name} where {query_to_delete} matches are found")
            self.collection.delete_many(query_to_delete)
        except Exception as e:
            logging.error("ERROR !!" + str(e))

    def drop_collection(self):
        """
        Drops a collection from an existing DB

        """
        try:
            logging.info(f"Dropping {self.collection_name} from {self.database}")
            self.collection.drop()
        except Exception as e:
            logging.error("ERROR !!" + str(e))