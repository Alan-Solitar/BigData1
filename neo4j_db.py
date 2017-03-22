import numpy as np
import pandas as pd
from py2neo import Graph, Node, Relationship
import sqlite3
import os
from time import sleep


class GeneInteractionManager():
    def __init__(self):
        self.graph = Graph()
        self.base_file_name = 'BIOGRID-MV-Physical-3.4.144.tab2'
        self.dir = os.path.dirname(os.path.abspath(self.base_file_name))
        self.base_with_path = os.path.join(self.dir,self.base_file_name)
        self.create_interaction_graph(self.base_with_path)
    
    def get_absolute_path(self,file_name):
        return os.path.join(self.dir,file_name)
    def main_method(self):
        val = int(input('what do you want do?\n1.Load File\n2.Query for interacting genes\n'))
        if val ==1:
            graph_file = input('path of file?: ')
            file_name = self.get_absolute_path(graph_file)
            self.reate_interaction_graph(file_name)
            self.get_node_count(graph)
        if val == 2:
            n = int(input('get interacting genes of degree: '))
            gene = input('Gene ID: ')
            self.get_nth_degree_genes(gene,n)
    #df = pd.read_csv(file_name, sep='\t')


    def create_interaction_graph(self,graph_file):
        try:
            self.graph.schema.create_index('Gene', 'gene_id')
        except:
            print('Graph is already indexed')

        query = '''lOAD CSV FROM "file:///{}" AS csvLine FIELDTERMINATOR '\\t'
                MERGE (a:Gene {{gene_id:csvLine[1]}}) 
                MERGE (b:Gene {{gene_id:csvLine[2]}})
                MERGE ((a)-[:INTERACTS_WITH]-(b))
                '''.format(graph_file)
        self.graph.run(query)

    def get_node_count(self):
        query = '''start n=node(*)
        match (n)
        return count(n)'''
        print(self.graph.data(query))


    def get_nth_degree_genes(self,gene,n):
        query = '''match (:Gene {{gene_id:"{}"}})-[:INTERACTS_WITH*..{}]->(a:Gene) 
        return distinct properties(a)'''
        query = query.format(gene,n)
        
        interacting_genes = self.graph.run(query).data()
        genes = [g['properties(a)']['gene_id'] for g in interacting_genes]
        print(len(genes))
        for gene in genes:
            print(gene)

#gim = GeneInteractionManager()
#gim.get_node_count()
#gim.get_nth_degree_genes(6416,2)   


