# Databricks notebook source
from pyspark import SparkConf,SparkContext
conf=SparkConf().setAppName("Student_Project")
sc=SparkContext.getOrCreate(conf=conf)

# COMMAND ----------


rdd.collect()

# COMMAND ----------

#removing the header
rdd=sc.textFile('/FileStore/tables/StudentData__1_.csv')
headers=rdd.first()
rdd=rdd.filter(lambda x:x!=headers)
rdd=rdd.map(lambda x:x.split(','))
rdd.collect()

# COMMAND ----------

#counting the number of student
rdd.count()


# COMMAND ----------

# total marks acchive by female and male
rdd2=rdd
rdd2=rdd.map(lambda x:(x[1],int(x[5])))
rdd2.reduceByKey(lambda x,y:x+y).collect()

# COMMAND ----------

#show total number of students passed and failed above 50 pass
rdd3=rdd
passed=rdd3.filter(lambda x:int(x[5])>50).count()
failed=rdd3.filter(lambda x:int(x[5])<50).count()
print(passed)
print(failed)

# COMMAND ----------

#show total number of students enrolled per course
rdd4=rdd
rdd4=rdd4.map(lambda x:(x[3],1))
rdd4.reduceByKey(lambda x,y:x+y).collect()


# COMMAND ----------

#total marks achieved per course
rdd5=rdd
rdd5=rdd5.map(lambda x:(x[3],int(x[5])))
rdd5.reduceByKey(lambda x,y:x+y).collect()

# COMMAND ----------

#average marks that students have achieved per course

rdd6=rdd
rdd6=rdd6.map(lambda x:(x[3],(int(x[5]),1)))
rdd6.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1])).collect()
# rdd6.map(lambda x:(x[0],x[1][0]/x[1][1])).collect()
rdd6.mapValues(lambda x:(x[0]/x[1])).collect()

# COMMAND ----------

#show minimum and maximum maeks achieved per course
rdd7=rdd
rdd7=rdd7.map(lambda x:(x[3],int(x[5])))
print(rdd7.reduceByKey(lambda x,y:x if x > y else y).collect())
print(rdd7.reduceByKey(lambda x,y:x if x < y else y).collect())



# COMMAND ----------

#show average age of male and Female students
rdd8=rdd
rdd8=rdd8.map(lambda x:(x[1],(int(x[0]),1)))
rdd8=rdd8.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
rdd8.mapValues(lambda x:x[0]/x[1]).collect()

