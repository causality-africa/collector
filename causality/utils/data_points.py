from decimal import Decimal
from typing import Optional


def upsert_data_point(
    geo_entity_id: int,
    indicator_id: int,
    source_id: int,
    date: str,
    numeric_value: Optional[float | Decimal | int],
    text_value: Optional[str],
    cursor,
) -> None:
    """Upsert a data point."""
    query = """
    INSERT INTO data_points (
        geo_entity_id, indicator_id, source_id,
        date, numeric_value, text_value
    )
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (geo_entity_id, indicator_id, source_id, date)
    DO UPDATE SET
        numeric_value = EXCLUDED.numeric_value,
        text_value = EXCLUDED.text_value
    """

    cursor.execute(
        query,
        (geo_entity_id, indicator_id, source_id, date, numeric_value, text_value),
    )
