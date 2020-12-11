__auteurs__ = "TOURE Almamy Moustapha, OUPRAXAY Philippe"
# initialisation


from pyspark import SparkContext
sc = SparkContext(master="local", appName="bigdata")

# lecture du RDD
bd = sc.textFile("sample.txt")
print(bd.collect())

# la phase de comptage
mot = bd.flatMap(lambda line: line.split())\
    .map(lambda word: (word, 1))
print(mot.collect())

# affichage cle valeur
motcount= mot.reduceByKey(lambda a, b: a + b)
print(motcount.collect())

# exportation du fichier
motcount.coalesce(1).saveAsTextFile("big_projet.txt")
