import os
import numpy as np

numVariables = 6
numIterations = 3
K = 3

'''
This may seem odd but the way I'm constructing the random centroids fits with the way I output them in my kmeans.java file
I add some number at the beginning of the string as a label in my kmeans.java so I add it here so as to not alter my kmeans code that strips it off
'''
cent = ""
for i in range(K):
	randomVector = np.random.rand(1,numVariables)
	vecString = ','.join(['%.5f' % num for num in randomVector.T])
	vecString = "1  " + vecString
	cent = cent + vecString + "\n"
text_file = open("centroids1.txt", "w")
text_file.write(cent)
text_file.close()



for i in range(0,numIterations):
	if(i > 0):
		os.system("hdfs dfs -rm -r testdir/centroids1.txt")
	os.system("hdfs dfs -put /home/mmclean/testdir/mr-app/centroids1.txt testdir")
	os.system("rm /home/mmclean/testdir/mr-app/.centroids1.txt.crc")
	os.system("hdfs dfs -rm -r KMOutput")
	os.system("yarn jar /home/mmclean/testdir/mr-app/target/mr-app-1.0-SNAPSHOT.jar kmeans testdir/std_data3.txt KMOutput "+str(K))
	os.system("rm /home/mmclean/testdir/mr-app/centroids1.txt")
	os.system("hdfs dfs -getmerge KMOutput /home/mmclean/testdir/mr-app/centroids1.txt")

os.system("mv /home/mmclean/testdir/mr-app/centroids1.txt /home/mmclean")
os.system("rm /home/mmclean/testdir/mr-app/.centroids1.txt.crc")
	
