from typing import Optional

from causality.models import Indicator


def get_indicator(code: str, cursor) -> Optional[Indicator]:
    """Get an indicator."""
    query = """
    SELECT id, code, name, category, unit, description, data_type
    FROM indicators
    WHERE code = %s
    """

    cursor.execute(query, (code,))
    result = cursor.fetchone()

    if result is None:
        return None

    return Indicator(
        id=result[0],
        code=result[1],
        name=result[2],
        category=result[3],
        unit=result[4],
        description=result[5],
        data_type=result[6],
    )


def get_or_create_indicator(
    code: str,
    name: str,
    category: str,
    description: str,
    unit: str,
    data_type: str,
    cursor,
) -> Indicator:
    """Get an indicator or create it if it doesn't exist."""
    query = """
    INSERT INTO indicators (code, name, category, description, unit, data_type)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (code) DO UPDATE SET
        name = EXCLUDED.name,
        category = EXCLUDED.category,
        description = EXCLUDED.description,
        unit = EXCLUDED.unit
    RETURNING id, code, name, category, description, unit, data_type
    """

    cursor.execute(
        query,
        (code, name, category, description, unit, data_type),
    )
    result = cursor.fetchone()

    return Indicator(
        id=result[0],
        code=result[1],
        name=result[2],
        category=result[3],
        description=result[4],
        unit=result[5],
        data_type=result[6],
    )
