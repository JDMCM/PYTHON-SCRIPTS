


import pyspark
import math
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as F
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql.functions import array
from pyspark.sql.functions import split
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType, IntegerType
sc = SparkContext('local')
spark = SparkSession(sc)
input1 = input('Enter file path: ').replace('"', '')
#df contains events and the time at which the events occurs
df = spark.read.text(input1)
df = df.withColumn('index', monotonically_increasing_id())
df = df.filter(df.index != 0)
split = split(df.value, ' ')
df = df.withColumn('part1Index', split.getItem(1).cast(DoubleType())) \
    .withColumn('part2Index', split.getItem(2).cast(DoubleType())) \
    .withColumn('time', split.getItem(3).cast(DoubleType())).drop('value')



#df3 is just particles in events and the time they colllide no correspadance with what it collides with
df1 = df.select('part1Index','time').withColumnRenamed('part1Index','part')
df2 = df.select('part2Index','time').withColumnRenamed('part2Index','part')
df3 = df1.unionAll(df2)
df3 = df3.sort(df.time.asc())


#part contains the number of times each particle colided with another particle
part = df3.groupBy('part').count()
part = part.sort('count',asceding=False)
part = part.withColumnRenamed('count','collison_num')
#part.sort('collison_num',ascending=False).show()

distr = part.groupBy('collison_num').count().sort('collison_num')
distr = distr.withColumn('index',floor(col('collison_num')/10)*10)\
    .groupBy('index').sum('count').sort('index')
#distr.show(40)



#evol will contain only unique collisons by particle
evol = df3.sort('time').dropDuplicates(['part']).sort('time').drop('part').withColumn('Unique Particle Collisons', monotonically_increasing_id())
#evol.show()

#avgtot will be average time between a particle colliding again
avga = df3.sort(['time'], ascending=True).distinct()

avgd = df3.sort(['time'], ascending=False).distinct()
# avga.show()



avgsum = avga.dropDuplicates(['part']).sort('part')

avgmis = avgd.dropDuplicates(['part']).sort('part')

avga = avga.subtract(avgsum).sort('part','time')
avgd = avgd.subtract(avgmis).sort('part','time')
# avga.show()
# avgd.show()

avga1 = avga.groupBy('part').count()
avga2 = avga.groupBy('part').sum('time')
avga = avga1.join(avga2, on=['part']).sort('part').withColumnRenamed('sum(time)','sump')
#avga.show()

avgd1 = avgd.groupBy('part').count()
avgd2 = avgd.groupBy('part').sum('time')
avgd = avgd1.join(avgd2, on=['part']).sort('part').drop('count').withColumnRenamed('sum(time)','sumn')

avgtot = avga.join(avgd,on='part').sort('part')
avgtot = avgtot.withColumn('Average Time between Collisons',F.col('sump')/ F.col('count')-F.col('sumn')/ F.col('count'))
avgtot = avgtot.withColumnRenamed('part', 'Particle Index')
avgtot = avgtot.select('Average Time between Collisons').sort('Average Time between Collisons')



#plotting
fig, axes = plt.subplots(nrows=2, ncols=2)

df = df.withColumnRenamed('index','Number of Events')
pdf = df.toPandas()
pdf.plot(ax=axes[0,0],kind = 'scatter', x='time', y='Number of Events', title='Number of Events versus Time')

pdistr = distr.toPandas()
pdistr.plot(ax=axes[0,1], kind='scatter', x='index' ,y='sum(count)', title='Number of Particles versus Collisons per Particle', xlabel='Collisons per Particle', ylabel='Particle Count') #kind = 'scatter', x = 'Collisons per Particle',y = 'Number of Particles', title='Number of Particles versus Collisons per Particle')

pevol = evol.toPandas()
pevol.plot(ax=axes[1,0], kind = 'scatter', x = 'time',y = 'Unique Particle Collisons' ,title='Number of Unique Collisons versus Time')

pavgtot = avgtot.toPandas()
pavgtot.plot(ax=axes[1,1], kind='hist', xlabel='Average Time between Collisons', ylabel="Particle Count", title='Average Time between Collisons')
plt.show()

