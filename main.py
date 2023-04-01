from CSVMongoPKG.CSVMongoModule import *

# Testing script

cd = CsvtoDb()
cd.establish_connection()
cd.create_collection()
cd.inserttoDb()

for i in cd.find_document({'Chiral indice n': '2'}):
    print(i)

cd.delete_document({'Chiral indice n': '2'})

for i in cd.find_document({'Chiral indice n': '2'}):
    print(i)

#cd.drop_collection()