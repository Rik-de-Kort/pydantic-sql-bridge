from pydantic import BaseModel

from pydantic_sql_bridge.utils import transform


class First(BaseModel):
    id: int
    name: str


class Second(BaseModel):
    id: int
    score: float


class Nested(BaseModel):
    id: int
    first: First
    second: Second


class Flat(BaseModel):
    id: int
    first_id: int
    first_name: str
    second_id: int
    second_score: float


class Mixed(BaseModel):
    id: int
    first_id: int
    first_name: str
    second: Second


def test_transform():
    nested = Nested(
        id=0, first=First(id=0, name="alice"), second=Second(id=1, score=-5.21)
    )
    flat = Flat(id=0, first_id=0, first_name="alice", second_id=1, second_score=-5.21)
    mixed = Mixed(
        id=0, first_id=0, first_name="alice", second=Second(id=1, score=-5.21)
    )
    assert transform(flat, Flat) == flat
    assert transform(flat, Mixed) == mixed
    assert transform(flat, Nested) == nested
    assert transform(mixed, Flat) == flat
    assert transform(mixed, Mixed) == mixed
    assert transform(mixed, Nested) == nested
    assert transform(nested, Flat) == flat
    assert transform(nested, Mixed) == mixed
    assert transform(nested, Nested) == nested
