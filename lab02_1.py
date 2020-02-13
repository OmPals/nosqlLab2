# Databricks notebook source
data = [1,2,3,4,5]

# COMMAND ----------

hello = sc.parallelize(data)

# COMMAND ----------

hello

# COMMAND ----------

print(hello)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orderdetails_tsv

# COMMAND ----------

firstRDD = sc.textFile("/FileStore/tables/input.txt")
lines = firstRDD.filter(lambda x: len(x)>0).flatMap(lambda x: x.split(" "))
wordCount = lines.map(lambda x: (x, 1)).reduceByKey(lambda x,y: x+y).sortByKey(True)
for word in wordCount.collect():
  print(word)

# COMMAND ----------

from pyspark.sql import *
dept1 = Row(dno='123456', name='computer science')
dept2 = Row(dno='123457', name='mech engg')
dept3 = Row(dno='123458', name='theatre and drama')
dept4 = Row(dno='123459', name='indoor recreation')

Employee = Row("ssn", "firstName", "lastName", "email", "salary", "dno")
emp1 = Employee('999-555-666', 'michael', 'armbrust', 'hello@bye.com', '98555', '123456')


# COMMAND ----------

emp_row_list = [emp1]
employee_df = spark.createDataFrame(emp_row_list)
employee_df.show()

# COMMAND ----------

dept_row_list = [dept1, dept2, dept3, dept4]
dept_df = spark.createDataFrame(dept_row_list)
dept_df.show()

# COMMAND ----------

employee_df.join(dept_df, employee_df.dno==dept_df.dno, 'inner').show()

# COMMAND ----------

d=spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header="true", inferSchema="true")
d.write.format("delta").save("/delta/diamonds")

# COMMAND ----------

d.show()
