


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
dfc = df.select('index','time').withColumnRenamed('index','Cumulative Event Count')



dfup = df.sort('time').dropDuplicates(['part1Index','part2Index'])
dfdwn = df.sort('time',ascending=False).dropDuplicates(['part1Index','part2Index']).withColumnRenamed('time','time1')
dfdiff = dfup.drop('index').join(dfdwn.drop('index'), on=['part1Index','part2Index'])
dfdiff = dfdiff.withColumn('collison length',col('time1')-col('time')).drop('time','time1')
#dfdiff.show()

#display the particle pairs with the most events and the time over all the events
dfwuh = df.groupBy('part1Index','part2Index').count().sort('count',ascending=False). \
    join(dfdiff,on=['part1Index','part2Index']).sort('count',ascending=False)
dfwuh.show()
print('Select indexes for detla t between events')
input2 = input('Enter first particle Index of a pair ').replace('"', '')
input3 = input('Enter second particle Index of a pair ').replace('"', '')
#get the time between events for particle pairs
dfdelta = df.filter((col('part1Index') == input2) & (col('part2Index') == input3)).sort('time').drop('index','part1Index','part2Index'). \
    withColumn('index', monotonically_increasing_id())
dfta = dfdelta.filter(dfdelta.index > 0.0).withColumn('newdex', monotonically_increasing_id()).drop('index'). \
    withColumnRenamed('time','time0')
dfdel = dfdelta.filter(col('index') < dfdelta.count()-2).withColumn('newdex',monotonically_increasing_id()).drop('index'). \
    withColumnRenamed('time','time1')
dfdelta = dfta.join(dfdel, on=['newdex']) \
    .withColumn('delta_t',col('time0')-col('time1')).drop('time0','time1') \
    .withColumnRenamed('newdex','event_count')

maxt = df.select(max('time')).collect()[0][0]
bin_num = 5000
dfo = df.withColumn('time-range', ceil(col('time')/(maxt/bin_num))*(maxt/bin_num)) \
    .select('time-range').groupBy('time-range').count().sort('time-range')

#for checking within timestep event distribution
# dfuh = dfo.withColumn('index',monotonically_increasing_id()%(bin_num/100)/(bin_num/500))
# dfuh = dfuh.groupBy('index').sum('count').sort('index')
#dfuh.show()




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
distr = distr.withColumn('index',floor(col('collison_num')/30)*30)\
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

dfo = dfo.withColumnRenamed('count','Event Count')
pdf = dfo.toPandas()
pdf.plot(ax=axes[1,0],kind = 'scatter', x='time-range', y='Event Count', title='Number of Events versus Time')

pdistr = distr.toPandas()
pdistr.plot(ax=axes[1,1], kind='bar', width=1, x='index' ,y='sum(count)', title='Number of Particles versus Collisons per Particle', xlabel='Collisons per Particle', ylabel='Particle Count') #kind = 'scatter', x = 'Collisons per Particle',y = 'Number of Particles', title='Number of Particles versus Collisons per Particle')

pevol = evol.toPandas()
pevol.plot(ax=axes[0,1], kind = 'scatter', x = 'time',y = 'Unique Particle Collisons' ,title='Number of Unique Collisons versus Time')

pdfc = dfc.toPandas()
pdfc.plot(ax=axes[0,0], kind='scatter', x='time',y='Cumulative Event Count', title='Cumulative Event Count vs. Time')

pdfdelta = dfdelta.toPandas()
pdfdelta.plot(kind='scatter', x='event_count', y='delta_t')

# pdfuh =dfuh.toPandas()
# pdfuh.plot(kind='scatter',x='index', y="sum(count)",xlabel='Fraction of Bigtimestep',ylabel='Event Count',title='Event counts sumed over fractions of bigtimestep')

plt.show()

