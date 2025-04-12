from typing import Optional

import pycountry

from causality.models import GeoEntity


def get_geo_entity(code: str, cursor) -> Optional[GeoEntity]:
    """Get a geographical entity."""
    query = """
    SELECT id, code, name, type
    FROM geo_entities
    WHERE code = %s
    """

    cursor.execute(query, (code,))
    result = cursor.fetchone()

    if result is None:
        return None

    return GeoEntity(id=result[0], code=result[1], name=result[2], type=result[3])


def get_or_create_geo_entity(code: str, name: str, e_type: str, cursor) -> GeoEntity:
    """Get a geographical entity or create it if it doesn't exist."""
    query = """
    INSERT INTO geo_entities (code, name, type)
    VALUES (%s, %s, %s)
    ON CONFLICT (code) DO UPDATE SET
        name = EXCLUDED.name,
        type = EXCLUDED.type
    RETURNING id, code, name, type
    """

    cursor.execute(query, (code, name, e_type))
    result = cursor.fetchone()

    return GeoEntity(id=result[0], code=result[1], name=result[2], type=result[3])


def upsert_relationship(
    parent_id: int, child_id: int, since: str, until: Optional[str], cursor
) -> None:
    """Upsert a geographic relationship."""
    query = """
    INSERT INTO geo_relationships (parent_id, child_id, since, until)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (parent_id, child_id) DO UPDATE SET
        since = EXCLUDED.since,
        until = EXCLUDED.until
    """

    cursor.execute(query, (parent_id, child_id, since, until))


def iso3_to_iso2(iso3_code) -> Optional[str]:
    """Convert ISO3 country code to ISO2."""
    try:
        country = pycountry.countries.get(alpha_3=iso3_code)
        return country.alpha_2 if country else None
    except (KeyError, AttributeError):
        return None
