from datetime import datetime
from typing import Annotated
from pydantic import BaseModel, Field, field_validator, ConfigDict

# Employee validation
class HiredEmployeesRow(BaseModel):
    model_config = ConfigDict(strict=True, extra='forbid', populate_by_name=True)
    id: int = Field(ge=1)
    name: str = Field(min_length=1, max_length=255)
    datetime_: Annotated[datetime, Field(strict=False)]
    department_id: int = Field(ge=1)
    job_id: int = Field(ge=1)

    @field_validator('name')
    @classmethod
    def name_not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError('name must not be blank')
        return value.strip()

# Employee batch model validation
class HiredEmployeesBatch(BaseModel):
    rows: list[dict] = Field(min_length=1, max_length=1000)

# Department validation
class DepartmentRow(BaseModel):
    model_config = ConfigDict(strict=True, extra='forbid')
    id: int = Field(ge=1)
    department: str = Field(min_length=1, max_length=255)

    @field_validator('department')
    @classmethod
    def department_not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError('department must not be blank')
        return value.strip()

# Department batch model validation
class DepartmentsBatch(BaseModel):
    rows: list[dict] = Field(min_length=1, max_length=1000)

# Job validation
class JobRow(BaseModel):
    model_config = ConfigDict(strict=True, extra='forbid')
    id: int = Field(ge=1)
    job: str = Field(min_length=1, max_length=255)

    @field_validator('job')
    @classmethod
    def job_not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError('job must not be blank')
        return value.strip()

# Job batch model validation
class JobsBatch(BaseModel):
    rows: list[dict] = Field(min_length=1, max_length=1000)