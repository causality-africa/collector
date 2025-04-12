from datetime import date, datetime

from causality.models import DataSource


def get_or_create_data_source(
    name: str, url: str, description: str, last_updated: date, cursor
) -> DataSource:
    """Get a data source or create it if it doesn't exist."""
    query = """
    INSERT INTO data_sources (name, url, description, last_updated)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (name) DO UPDATE SET
        url = EXCLUDED.url,
        description = EXCLUDED.description,
        last_updated = EXCLUDED.last_updated
    RETURNING id, name, url, description, last_updated
    """

    cursor.execute(
        query,
        (name, url, description, last_updated),
    )
    result = cursor.fetchone()

    return DataSource(
        id=result[0],
        name=result[1],
        url=result[2],
        description=result[3],
        last_updated=result[4],
    )


def get_derived_data_source(cursor) -> DataSource:
    return get_or_create_data_source(
        "Derived",
        "https://causality.africa/derived/",
        "Data computed based on existing data",
        datetime.now().date(),
        cursor,
    )
