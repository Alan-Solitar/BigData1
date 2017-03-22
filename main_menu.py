from neo4j_db import GeneInteractionManager 
from pg_db import PatientDataManager

print('One moment please.  Databases are being created.')

# create class objects
pdm = PatientDataManager()
gim = GeneInteractionManager()

print('Databases are ready')

# menu related
intro_phrase = 'Please type the number corresponding to the query you want to make.\n  The option to load files will be presented later.'
menu_options = '1. Query1\n2. Query2\n3.Query3\n4. Query4\nPress 0 to exit\n'
print(intro_phrase)

while(True):
    response = int(input(menu_options))
    print(response)
    if response == 0:
        exit()
    if response == 1:
        gim.main_method()
    if response == 2:
        pass
    if response == 3:
        pass
    if response == 4:
        pdm.main_method()


