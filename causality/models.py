from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass
class GeoEntity:
    id: int
    code: str
    name: str
    type: str


@dataclass
class GeoEntityMeta:
    geo_entity_id: int
    key: str
    value: str


@dataclass
class GeoRelationship:
    parent_id: int
    child_id: int
    since: date
    until: Optional[date]


@dataclass
class Indicator:
    id: int
    code: str
    name: str
    category: str
    unit: str
    description: str
    data_type: str


@dataclass
class DataSource:
    id: int
    name: str
    url: str
    description: str
    last_updated: date


@dataclass
class DataPoint:
    id: int
    geo_entity_id: int
    indicator_id: int
    source_id: int
    date: date
    numeric_value: Optional[float]
    text_value: Optional[str]
