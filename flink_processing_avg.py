import os
from venv import create

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings
from pyflink.table.descriptors import Schema, Kafka, Json
from pyflink.table.window import Tumble,Over
from pyflink.table.expressions import lit ,col,timestamp_diff
from pyflink.table import expressions as expr
from sqlalchemy import DATE
from pyflink.table.types import *


def main2():
    # Create streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()

    settings = EnvironmentSettings.new_instance()\
                      .in_streaming_mode()\
                      .use_blink_planner()\
                      .build()

    # create table environment
    tbl_env = StreamTableEnvironment.create(stream_execution_environment=env,
                                            environment_settings=settings)
    
    
    # add kafka connector dependency
    kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                            'flink-sql-connector-kafka_2.11-1.13.0.jar')


    tbl_env.get_config()\
            .get_configuration()\
            .set_string("pipeline.jars", "file:///{}".format(kafka_jar))   

    src_ddl = """
        CREATE TABLE IPull (
                `type` STRING,
                `repo_name` STRING,
                `repo_id` INT,
                `created_at` BIGINT,
                `proctime` AS PROCTIME()                
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'InputPull',
            'properties.bootstrap.servers' = 'localhost:29092',
            'format' = 'json'
        )
    """
    
    
    tbl_env.execute_sql(src_ddl)

    # create and initiate loading of source Table
    tbl = tbl_env.from_path('IPull')

    print('\nSource Schema')
    tbl.print_schema()
    print('\nSource Data')
    print(tbl.to_pandas)

    
    
    #tbl.filter(col('repo_id') == 354314530).select(col('updated_at').avg.alias("average_time")).execute().print()
    #tbl.where(col('repo_id') == 354314530).select(col('updated_at').avg.alias("average_time")).execute().print()

    #operation=tbl.where(col('repo_id') == 354314530).select(col('created_at').avg.alias('average_time'))
 
    operation=tbl.over_window(Over.partition_by(tbl.repo_id).order_by(tbl.proctime) \
   .preceding(expr.UNBOUNDED_RANGE) \
   .alias("w"))\
    .select( tbl.created_at.avg.over(col('w')).alias('average_time'),tbl.repo_id)

        
    
    print('\nProcess Sink Schema')
    
    operation.print_schema()
    
    print('\nSink Data')
    
    print(operation.to_pandas)
    
    sink_ddl = """
        CREATE TABLE oPull (
                `average_time` BIGINT,
                `repo_is` INT
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'outputPull',
            'properties.bootstrap.servers' = 'localhost:29092',
            'format' = 'json'
        )
    """
    tbl_env.execute_sql(sink_ddl)

    
    operation.execute_insert('oPull').wait()
    
    
    tbl_env.execute(operation).insert_into("oPull")
    
if __name__ == '__main__':
    main2()