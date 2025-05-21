from datetime import datetime, timezone

def parse_ts_to_millis(raw_ts):
    """
    Parsuje timestamp w ms z raw_ts, który może być:
      - liczbą (int/float) — już ms,
      - stringiem w formacie "YYYY-MM-DD HH:MM:SS",
      - stringiem z ułamkiem sekund: "YYYY-MM-DD HH:MM:SS.fff" lub "YYYY-MM-DD HH:MM:SS.f",
      - stringiem ISO (np. T‑separator).
    """
    # 1) Jeśli już liczba, zwróć od razu
    if isinstance(raw_ts, (int, float)):
        return int(raw_ts)

    # 2) Spróbuj fromisoformat (Python ≥3.7, od 3.11 obsługuje większość wariantów ISO) :contentReference[oaicite:0]{index=0}
    try:
        # datetime.fromisoformat akceptuje 'YYYY-MM-DD[T ]HH:MM:SS[.ffffff][+HH:MM]'
        dt = datetime.fromisoformat(raw_ts)
        # ustaw na UTC, jeśli jest naive
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except (ValueError, TypeError):
        pass

    # 3) Użyj strptime z %f, który dopasowuje od 1 do 6 cyfr ułamka  obsłuży zarówno ".0", jak i ".000" :contentReference[oaicite:1]{index=1}
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(raw_ts, fmt)
            dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue

    # 4) Jeśli wciąż nic, zwróć 0 lub rzuć wyjątek
    raise ValueError(f"Niepoprawny format daty: {raw_ts}")
