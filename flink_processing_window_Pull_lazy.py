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

    
    tbl = tbl_env.from_path('IPull')

    print('\nSource Schema')
    tbl.print_schema()
    print('\nSource Data')
    print(tbl.to_pandas)

  
    
    operation=tbl.window(Tumble.over(lit(minutes).second).on('proctime').alias("w")).group_by(col('type'),col('w')).select( col('type').count.alias('count'),col('type'))
                
    print('\nProcess Sink Schema')

    operation.print_schema()
    print('\nSink Data')
    #print(windowed_rev.to_pandas)
    
    ###############################################################
    # Create Kafka Sink Table
    ###############################################################
    sink_ddl = """
        CREATE TABLE oPull (
                `count` BIGINT NOT NULL,
                `type` STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'outputPull',
            'properties.bootstrap.servers' = 'localhost:29092',
            'format' = 'json'
        )
    """
    tbl_env.execute_sql(sink_ddl)

    # write time windowed aggregations to sink table
    
    operation.execute_insert('oPull').wait()
    
    tbl_env.execute(operation).insert_into("oPull")

if __name__ == '__main__':
    
    main()
