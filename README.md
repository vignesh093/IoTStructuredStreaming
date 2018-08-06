This repo does below things

  1.After metadata validation, it loads the timeseries data to HBASE.
  
  2.If metadata validation fails, loads into a separate HBASE table.
  
  3.Calcualtes average variable value for each variable based on the window applied and written to Kafka.
  
  4.Device status(ALIVE/DEAD) for each device is calculated using stateful operation and written to Kafka.
