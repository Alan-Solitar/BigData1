import numpy as np
import pandas as pd
from py2neo import Graph, Node, Relationship
import sqlite3
from time import sleep




def get_gene(gene):
    if gene in nodes.keys():
        return nodes[gene]
    else: 
        node = Node('gene',gene_id= str(gene))
        nodes[gene] = node
        return node
nodes = {}
file_name = 'BIOGRID-MV-Physical-3.4.144.tab2'
df = pd.read_csv(file_name, sep='\t')

gene_a = 'Entrez Gene Interactor A'
gene_b = 'Entrez Gene Interactor B'
current_df = df[[gene_a, gene_b]]
current_df.info()
current_df.to_csv(file_name + '.csv',index=False)
#print(current_df.shape)
#sleep(444)
graph = Graph()
#graph.schema.create_uniqueness_constraint("gene", "gene_id")

#tx = graph.begin()


def create_interaction_graph():
    try:
        graph.schema.create_index('Gene', 'gene_id')
    except:
        print('Graph is already indexed')

    graph.run('''lOAD CSV FROM "file:///media/sf_Share_Nyaa/data/BIOGRID-MV-Physical-3.4.144.tab2.csv" AS csvLine
            MERGE (a:Gene {gene_id:csvLine[0]}) 
            MERGE (b:Gene {gene_id:csvLine[1]})
            MERGE ((a)-[:INTERACTS_WITH]-(b))
            ''')
#query = 'MATCH (a:gene {gene_id:"{}"})-[*1..5]-(nth_degrees)'
 #   'RETURN DISTINCT nth_degrees'.format(gene_id)

def get_node_count():
    query = '''start n=node(*)
    match (n)
    return count(n)'''
    print(graph.data(query))


#for index,row in current_df.iterrows():
    #print(row[0],row[1])
    #sleep(100)
 #   gene_a = row[0]
  #  gene_b = row[1]
   # graph.run('''MERGE (a:Gene{gene_id:{gene_id_a}}) 
   #     MERGE (b:Gene{gene_id:{gene_id_b}} )
    #    CREATE (a)-[:INTERACTS_WITH]->(b)
    #    CREATE (b)-[:INTERACTS_WITH]->(a)''',
     #   gene_id_a=int(gene_a), gene_id_b = int(gene_b) )




#graph.data(query)
#get_nth_degree_genes('6416')   

#def readCSV_forNeo4j_Trump(path):
    #with open(path + '/Trump/TrumpWorldData_Person-Org.csv', ncoding='utf8') as csvfile:
    #reader = csv.DictReader(csvfile)
    #for row in reader:
     #   Organization = row['Organization']
     #   Person = row['Person']
     #   Connection = row['Connection']
     #   graph.run('''MERGE (p:Person{name:{name}}) 
     #   MERGE (o:Organization{name:{organization}} )
     #   CREATE (p)-[:IS_CONNECTED{type:{relationship}}]->(o)''',
     #   name=Person, organization=Organization , relationship = Connection )

def get_nth_degree_genes(gene,graph,n):
    query = '''match (:Gene {{gene_id:"{}"}})-[:INTERACTS_WITH*..{}]-(a:Gene) 
    return distinct properties(a)'''
    query = query.format(gene,n)
    
    interacting_genes = graph.run(query).data()
    genes = [g['properties(a)']['gene_id'] for g in interacting_genes]
    print(len(genes))
    sleep(55555)
    
    #print(graph.data(query))
#create_interaction_graph()
n = input('get interacting genes of degree: ')
get_node_count()
get_nth_degree_genes(6416,graph,n)