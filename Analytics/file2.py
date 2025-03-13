
from pyspark.sql.functions import hour

def evening_riders(tablename):
    df = tablename.withColumn('Hour', hour(tablename['accept_time']))
    df = df.select('Hour','rider_id')
    df = df.filter( (df['Hour']>=16) & (df['Hour']<=22) )
    final = df.select('rider_id').distinct()
    return  final