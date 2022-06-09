from pyspark.sql import SparkSession
from pyspark.sql import functions as F

class Story:
    def __init__(self, name: str, capacity: int,
        opens: int, closes: int ):
        self.name = name
        self.capacity = capacity
        self.opens = opens
        self.closes = closes
    
    def _list(self):
        return [self.name, self.capacity, self.opens, self.closes]

class StoreOccupants:
    def __init__(self,storename: str, occupants: int):
        self.storename = storename
        self.occupants = occupants
    def _list(self):
        return [self.storename, self.occupants]

stores = [
    Story("a", 24, 28, 20),
    Story("b", 37, 7, 21),
    Story("c", 18, 5, 23)
]
val_stores = [store._list() for store in stores]

occupants = [
    StoreOccupants("a", 8),
    StoreOccupants("b", 20),
    StoreOccupants("c", 16),
    StoreOccupants("d", 55),
    StoreOccupants("e", 8)
]
val_occupants = [occupant._list() for occupant in occupants]

spark = SparkSession.builder.appName('sparkdf').getOrCreate()

df = (spark.createDataFrame(val_stores)
        .toDF("name", "capacity", "opens", "closes"))
df.createOrReplaceTempView("stores")

occupancy = spark.createDataFrame(occupants)
occupancy.createOrReplaceTempView("store_occupants")

#using sql
spark.sql("select * from stores where closes >= 22").show()

#using DataFrame
df.where(df["closes"] >= 22).show()
df.where(F.col("closes") >= 22).show()
df.where('closes >= 22').show()

#Projection
df.select("name").where('capacity > 20').show()

#Inner Join
inner = (
    df.join(occupancy)
    .where(df["name"] == occupancy["storename"])
)
inner.show()

#Right Join
right = (
    df.join(occupancy,df["name"] == occupancy["storename"],
    "right")
)
right.show()

#Left Join
left = (
    df.join(occupancy,df["name"] == occupancy["storename"],
    "left")
)
left.show()

#Semi-Join
boutiques =(
df.select(df["name"].alias("boutiquename"))
    .where('capacity > 20')
)
boutiques.createOrReplaceTempView("boutiques")
boutiques.show()

semiJoin = (
    df.join(boutiques,
    df["name"] == boutiques["boutiquename"],
    "semi")
)
semiJoin.show()

#Anti-Join
antiJoin = (
    df.join(boutiques,
    df["name"] == boutiques["boutiquename"],
    "anti")
)
antiJoin.show()

#In Operator
inOper = spark.sql(
"""
select * from stores
where stores.`name` in (
  select boutiquename from boutiques
)
"""
)
inOper.explain("formatted")

#Not In
notInOper = spark.sql(
"""
select * from stores
where stores.`name`
not in (
  select boutiquename from boutiques
)
"""
)
notInOper.show()

#Full-Join
addStores = spark.createDataFrame(
    [
        ["f", 42, 5, 23],
        ["g", 19, 7, 18]
    ]).toDF("name","capacity","opens","closes")

fullJoined = (
    df.union(addStores)
  .join(occupancy,
    df["name"] == occupancy["storename"],
    "full")
)
fullJoined.show()

#Exercise 4.3
partySize = 4
hasSeats = (df
    .join(occupancy, df["name"] == occupancy["storename"])
    .withColumn("availability", df["capacity"]
    -occupancy["occupants"])
)
hasSeats.show()

hasSeats = (hasSeats
    .where(hasSeats["availability"] >= partySize)
    .select("name","availability")
)
hasSeats.show()
