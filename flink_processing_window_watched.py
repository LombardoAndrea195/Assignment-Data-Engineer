import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings
from pyflink.table.descriptors import Schema, Kafka, Json
from pyflink.table.window import Tumble,Over
from pyflink.table.expressions import lit ,col
from pyflink.table import expressions as expr



def main2(minutes):
    # Create streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()

    settings = EnvironmentSettings.new_instance()\
                      .in_streaming_mode()\
                      .use_blink_planner()\
                      .build()

    # create table environment
    tbl_env = StreamTableEnvironment.create(stream_execution_environment=env,
                                            environment_settings=settings)
    
    tbl_env.get_config().get_configuration().set_integer("python.fn-execution.bundle.size", 300000)
    tbl_env.get_config().get_configuration().set_integer("python.fn-execution.bundle.time", 10)
    tbl_env.get_config().get_configuration().set_integer("table.exec.source.idle-timeout", 5)
    tbl_env.get_config().get_configuration().set_integer("python.fn-execution.arrow.batch.size",
                                                       10000)
    # add kafka connector dependency
    kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                            'flink-sql-connector-kafka_2.11-1.13.0.jar')


    tbl_env.get_config()\
            .get_configuration()\
            .set_string("pipeline.jars", "file:///{}".format(kafka_jar))
 
   

      #######################################################################
    # Create Kafka Source Table with DDL
    #
    # - The Table API Descriptor source code is undergoing a refactor
    #   and currently has a bug associated with time (event and processing)
    #   so it is recommended to use SQL DDL to define sources / sinks
    #   that require time semantics.
    #   - http://apache-flink-mailing-list-archive.1008284.n3.nabble.com/DISCUSS-FLIP-129-Refactor-Descriptor-API-to-register-connector-in-Table-API-tt42995.html
    #   - http://apache-flink-user-mailing-list-archive.2336050.n4.nabble.com/PyFlink-Table-API-quot-A-group-window-expects-a-time-attribute-for-grouping-in-a-stream-environment--td36578.html
    #######################################################################

    src_ddl = """
        CREATE TABLE Iwatch (
                `type` STRING,
                `repo_name` STRING,
                `repo_id` INT,
                `created_at` BIGINT,
                `proctime` AS PROCTIME()                
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'InputWatch',
            'properties.bootstrap.servers' = 'localhost:29092',
            'format' = 'json'
        )
    """
    
    
    tbl_env.execute_sql(src_ddl)

    # create and initiate loading of source Table
    tbl = tbl_env.from_path('Iwatch')

    print('\nSource Schema')
    tbl.print_schema()
    print('\nSource Data')
    print(tbl.to_pandas)

    #####################################################################
    # Define Tumbling Window Aggregate Calculation of Revenue per Seller
    #
    # - for every 30 second non-overlapping window
    # - calculate the revenue per seller
    #####################################################################
    
  
    
    #tbl.window(Tumble.over(lit(3).second).on('proctime').alias("w")).group_by(col('type'),col('w')).select( col('type').count.alias('count'),col('type')).execute().print()

    operation=tbl.over_window(Over.partition_by(tbl.type).order_by(tbl.proctime) \
    .preceding(lit(minutes).seconds).alias("ow")) \
    .select(tbl.type.count.over(col('ow')), tbl.type)
    #operation=tbl.over_window(Tumble.over(lit(minutes).second).on('proctime').alias("w")).group_by(col('type'),col('w')).select( col('type').count.alias('count'),col('type'))
                
    print('\nProcess Sink Schema')

    operation.print_schema()
    print('\nSink Data')
    #print(windowed_rev.to_pandas)
    
    ###############################################################
    # Create Kafka Sink Table
    ###############################################################
    sink_ddl = """
        CREATE TABLE oWatch (
                `count` BIGINT NOT NULL,
                `type` STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'outputWatch',
            'properties.bootstrap.servers' = 'localhost:29092',
            'format' = 'json'
        )
    """
    tbl_env.execute_sql(sink_ddl)

    # write time windowed aggregations to sink table
    
    operation.execute_insert('oWatch').wait()
    
    tbl_env.execute(operation).insert_into("oWatch")

if __name__ == '__main__':
    
    main2(5)
