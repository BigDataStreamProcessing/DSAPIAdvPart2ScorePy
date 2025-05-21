from pyflink.common.typeinfo import Types

from testdata.inputs import Inputs
from tools.environment import get_environment
from tools.properties import load_properties
from tools.sinks_and_sources import get_source_data_stream


def main():
    cfg = load_properties()

    env = get_environment(cfg)

    score_event_ds = get_source_data_stream(env, cfg, Inputs.get_json_unordered_strings())

    result_ds = score_event_ds.map(lambda se: 1, output_type=Types.LONG())

    result_ds.print()

    env.execute("Score Events Delay Analysis")


if __name__ == '__main__':
    main()
