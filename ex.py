import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Exercises").getOrCreate()

sc = spark.sparkContext


# 1 CREATE RDD
# 1.1 From a list/tuple/set/dict(key mapped only)
# list 
nums_list = [1, 2, 3, 4, 5, 6]
list_rdd = sc.parallelize(nums_list)
print("List : ")
print(list_rdd.collect())

# tuple
nums_tuple = (1, 2 ,3, 4, 5)
tuple_rdd = sc.parallelize(nums_tuple)
print("Tuple : ")
print(tuple_rdd.collect())

# set
nums_set = set([1, 2, 2, 1])
set_rdd = sc.parallelize(nums_set)
print("Set : ")
print(set_rdd.collect())

# dictionary
employee = {
    "first_name": "employee_first_name",
    "last_name": "employee_last_name"
}

employee_dict_rdd = sc.parallelize(employee)
print("Dictionary : ")
print(employee_dict_rdd.collect())

# 1.2 From a local file
hybris_products = sc.textFile("HYBRIS_PRODUCTS.csv")
print("Reading from a file : ")
print(hybris_products.take(10))

# 1.3 From another RDD:
# 1.4 Create pair RDD from RDD