from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment


def get_environment(cfg):
    parallelism = int(cfg["env"]["parallelism"])
    jars = cfg['java_dependencies']['jars']

    config = Configuration()
    config.set_string("python.fn-execution.bundle.time", "10")
    config.set_string("pipeline.jars", jars)

    # 2. Środowisko Flink
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(parallelism)

    return env