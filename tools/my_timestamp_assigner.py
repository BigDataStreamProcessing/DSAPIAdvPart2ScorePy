from pyflink.common.watermark_strategy import TimestampAssigner

class MyTimestampAssigner(TimestampAssigner):
    """
    Przypisuje event-time bazując na polu `ts` w obiekcie ScoreEvent.
    """
    def extract_timestamp(self, element, record_timestamp):
        # element.ts powinno być w milisekundach od epoki
        return element.ts
