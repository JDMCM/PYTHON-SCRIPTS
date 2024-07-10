


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
from pyspark.sql import Window
sc = SparkContext('local')
spark = SparkSession(sc)
input1 = input('Enter file path: ').replace('"', '')
#df contains events and the time at which the events occurs


df = spark.read.text(input1)
df = df.withColumn('index', monotonically_increasing_id())
df = df.filter(df.index != 0)




split = split(df.value, ' ')

df = df.withColumn('p1', split.getItem(1).cast(DoubleType())) \
    .withColumn('p2', split.getItem(2).cast(DoubleType())) \
    .withColumn('time', split.getItem(3).cast(DoubleType()))

if split.__sizeof__() > 5:
    df = df.withColumn('p1x', split.getItem(4).cast(DoubleType())) \
        .withColumn('p1y', split.getItem(5).cast(DoubleType())) \
        .withColumn('p1z', split.getItem(6).cast(DoubleType())) \
        .withColumn('p1vx', split.getItem(7).cast(DoubleType())) \
        .withColumn('p1vy', split.getItem(8).cast(DoubleType())) \
        .withColumn('p1vz', split.getItem(9).cast(DoubleType())) \
        .withColumn('p1r', split.getItem(10).cast(DoubleType())) \
        .withColumn('p2x', split.getItem(11).cast(DoubleType())) \
        .withColumn('p2y', split.getItem(12).cast(DoubleType())) \
        .withColumn('p2z', split.getItem(13).cast(DoubleType())) \
        .withColumn('p2vx', split.getItem(14).cast(DoubleType())) \
        .withColumn('p2vy', split.getItem(15).cast(DoubleType())) \
        .withColumn('p2vz', split.getItem(16).cast(DoubleType())) \
        .withColumn('p2r', split.getItem(17).cast(DoubleType())) 

df = df.drop('value').drop('index')

w = Window.orderBy(lit('A'))
df = df.withColumn('index', row_number().over(w))



dfc = df.select('index','time').withColumnRenamed('index','Cumulative Event Count')
dfc.show()


dfsimp = df.select('p1','p2','time')

dfup = dfsimp.sort('time').dropDuplicates(['p1','p2'])
dfdwn = dfsimp.sort('time',ascending=False).dropDuplicates(['p1','p2']).withColumnRenamed('time','time1')
dfdiff = dfup.drop('index').join(dfdwn.drop('index'), on=['p1','p2'])
dfdiff = dfdiff.withColumn('collison length',col('time1')-col('time')).drop('time','time1')
#dfdiff.show()

#display the particle pairs with the most events and the time over all the events
dfwuh = df.groupBy('p1','p2').count().sort('count',ascending=False). \
    join(dfdiff,on=['p1','p2']).sort('count',ascending=False)
dfwuh.show()
print('Select indexes for detla t between events')
input2 = input('Enter first particle Index of a pair ').replace('"', '')
input3 = input('Enter second particle Index of a pair ').replace('"', '')
#get the time between events for particle pairs

dfdelta = df.filter((col('p1') == input2) & (col('p2') == input3)).sort('time').drop('index','p1','p2'). \
    withColumn('index', monotonically_increasing_id())

if split.__sizeof__() > 5:
    dfdelta = dfdelta.withColumn('sect', ((col('p1x') - col('p2x'))**2 + (col('p1y') - col('p2y'))**2 + (col('p1z') - col('p2z'))**2)**0.5 < col('p1r')+col('p2r')) \
        .select('index','time','sect')
dfta = dfdelta.filter(dfdelta.index > 0.0).withColumn('newdex', monotonically_increasing_id()).drop('index'). \
    withColumnRenamed('time','time0')
if split.__sizeof__() > 5:
   dfta = dfta.drop('sect')
dfdel = dfdelta.filter(col('index') < dfdelta.count()-2).withColumn('newdex',monotonically_increasing_id()).drop('index'). \
    withColumnRenamed('time','time1')
dfdelta = dfta.join(dfdel, on=['newdex']) \
    .withColumn('delta_t',col('time0')-col('time1')).drop('time0','time1') \
    .withColumnRenamed('newdex','event_count')

if split.__sizeof__() > 5:
    dfsect = dfdelta.filter(col('sect'))#.drop('sect')
    dfpass = dfdelta.filter(col('sect') != True)#.drop('sect')

    

maxt = df.select(max('time')).collect()[0][0]
bin_num = 5000
dfo = df.withColumn('time-range', ceil(col('time')/(maxt/bin_num))*(maxt/bin_num)) \
    .select('time-range').groupBy('time-range').count().sort('time-range')

#for checking within timestep event distribution
# dfuh = dfo.withColumn('index',monotonically_increasing_id()%(bin_num/100)/(bin_num/500))
# dfuh = dfuh.groupBy('index').sum('count').sort('index')
#dfuh.show()




#df3 is just particles in events and the time they colllide no correspadance with what it collides with
df1 = df.select('p1','time').withColumnRenamed('p1','part')
df2 = df.select('p2','time').withColumnRenamed('p2','part')
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

if split.__sizeof__() > 5:
    pdfsect = dfsect.toPandas()
    pdfpass = dfpass.toPandas()

    ax = pdfsect.plot(kind='scatter', x='event_count', y='delta_t',c='red')
    pdfpass.plot(ax=ax, kind='scatter', x='event_count', y='delta_t')
else:
    pdfdelta = dfdelta.toPandas()
    pdfdelta.plot(kind='scatter', x='event_count', y='delta_t')


# pdfuh =dfuh.toPandas()
# pdfuh.plot(kind='scatter',x='index', y="sum(count)",xlabel='Fraction of Bigtimestep',ylabel='Event Count',title='Event counts sumed over fractions of bigtimestep')

plt.show()

