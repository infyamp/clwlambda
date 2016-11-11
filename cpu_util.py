import sys
import logging
import pymysql
import boto3
import datetime
ec = boto3.client('cloudwatch')  

def lambda_handler(event, context):
    rds_host  = "testlambda.cb6ekvmpkwu3.ap-southeast-2.rds.amazonaws.com"
    name = "testlambda"
    password = "testlambda"
    db_name = "testlambda"
    port = 3306
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    systime =  datetime.datetime.now()
    print systime
    try:
        conn = pymysql.connect(host=rds_host, user=name, passwd=password, db=db_name, port=port)
        logger.info("SUCCESS: Connection to RDS mysql instance succeeded")
    except:
        logger.error("ERROR:")
        sys.exit() 
        
    listingInstanceID =ec.list_metrics( 
        Namespace='AWS/EC2',
        MetricName='CPUUtilization',
        Dimensions=[
            {
                'Name': 'InstanceId',
            }
        ]
        )
  #  print ("list metrics %s", listingInstanceID)
    for metrics in listingInstanceID['Metrics']:
        for tags in metrics['Dimensions']:
            if tags['Name'] == 'InstanceId' :
                instanceId= tags['Value']
        CPU = ec.get_metric_statistics(
        Namespace='AWS/EC2',
        MetricName='CPUUtilization', 
        #StartTime=timestamp, #2016-10-24+"T"+05:10:1477287548 time change format with respect to datetime funxtion timestamp = datetime.datetime.now()   5/10/2016 8:55
        StartTime='2016-11-01T01:00:00',
        EndTime='2016-11-01T04:00:00',
     #   StartTime=startTime,
      #  EndTime=endTime,
        Period=3600,
        Statistics=["Maximum"],
        Dimensions= [
        {
        'Name' : 'InstanceId',
        'Value' : instanceId, 
        }
        ],
        Unit='Percent'
        )
        item_count_cpu=0
        print ("Utilization values %s", CPU)
        for var in CPU['Datapoints']:
            print ("each datapoint",var)
            if 'Timestamp' in var :
                Timestamp=var['Timestamp']
                Maximum=var['Maximum']
                print Timestamp, Maximum
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO testlambda.CPU_measurement ( `Instanceid`, `Metrictype`, `Category`, `MetricUnit`, `MaxValue` , `TimeStamp` , `systime`) VALUES ( %s, 'CPUUtilization', 'AWS/EC2', 'Percent', %s, %s, %s)", ( instanceId, Maximum, Timestamp , systime))
     #           (`Instance-id`,`Mount`,`Filesystem`,`Timestamp`,`Metric-Unit`,`Category`,`Metric-type`,`systime`) VALUES 
    #('i-df4e7a03','/home','/dev/mapper/vgvdc-home',current_timestamp(),'Percent','Linux System','DiskSpaceUtilization','')")
                    cur.execute("SELECT * FROM  testlambda.CPU_measurement")
                    for row in cur:
                        item_count_cpu += 1
                      #  logger.info(row)
                        print("Each row %s", row)
    
        #    Maximum=''
        #    Timestamp=''
    conn.commit()
    return "Listed %d items from RDS MySQL table" %(item_count_cpu)



