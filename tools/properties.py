import argparse
import configparser

def load_properties():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        default="flink.properties",
        help="Ścieżka do pliku .properties"
    )
    args = parser.parse_args()

    # Wczytaj ustawienia
    cfg = configparser.ConfigParser()
    cfg.read(args.config)

    return cfg
