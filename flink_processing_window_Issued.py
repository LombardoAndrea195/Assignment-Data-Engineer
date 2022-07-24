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
        CREATE TABLE IIssues (
                `type` STRING,
                `repo_name` STRING,
                `repo_id` INT,
                `created_at` BIGINT,
                `proctime` AS PROCTIME()                
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'InputIssues',
            'properties.bootstrap.servers' = 'localhost:29092',
            'format' = 'json'
        )
    """
    
    
    tbl_env.execute_sql(src_ddl)

    
    tbl = tbl_env.from_path('IIssues')

    print('\nSource Schema')
    tbl.print_schema()
    print('\nSource Data')
    print(tbl.to_pandas)

    
    operation=tbl.over_window(Over.partition_by(tbl.type).order_by(tbl.proctime) \
    .preceding(lit(minutes).seconds).alias("ow")) \
    .select(tbl.type.count.over(col('ow')), tbl.type)           
    print('\nProcess Sink Schema')

    operation.print_schema()
    print('\nSink Data')
    
    sink_ddl = """
        CREATE TABLE oIssues (
                `count` BIGINT NOT NULL,
                `type` STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'ouputIssues',
            'properties.bootstrap.servers' = 'localhost:29092',
            'format' = 'json'
        )
    """
    tbl_env.execute_sql(sink_ddl)

    
    
    operation.execute_insert('oIssues').wait()
    
    tbl_env.execute(operation).insert_into("oIssues")

if __name__ == '__main__':
    
    main2(1)
