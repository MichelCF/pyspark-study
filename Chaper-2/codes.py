from pyspark.sql import SparkSession
from pyspark.sql import functions as F

class Activity:
	def __init__(self, userId: str, cartId: str, itemId: str):
		self.userId = userId
		self.cartId = cartId
		self.itemId = itemId
	
	def _list(self):
		return[self.userId, self.cartId, self.itemId]

activities = [
	Activity('u1','c1','i1'),
	Activity('u1','c1','i2'),
	Activity('u2','c2','i1'),
	Activity('u3','c3','i3'),
	Activity('u4','c4','i3')
	]

list_activities = [activity._list() for activity in activities]

spark = SparkSession.builder.appName('sparkdf').getOrCreate()

df = (spark.createDataFrame(list_activities)
		.toDF("user_id", "cart_id", "item_id"))

#How to get the elements of a column without repetition?
users = df.select("user_id").distinct()
users.show()

#Calculate the Daily Average Number of Items Across All User Carts
avg_cart_items = (
	df.select("cart_id","item_id").groupBy("cart_id")
		.agg(F.count(F.col("item_id")).alias("total"))
		.agg(F.avg(F.col("total")).alias("avg_cart_items"))
)
avg_cart_items.show()

#Generate the Top Ten Most Added Items Across All User Carts
(
	df.select("item_id")
	.groupBy("item_id")
	.agg(F.count(F.col("item_id")).alias("total"))
	.sort(F.desc("total"))
	.limit(10).show()
)