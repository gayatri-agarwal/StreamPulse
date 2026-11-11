"""Validated API schemas for experiment definitions and runs."""

from enum import Enum
from typing import Dict, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from app.processing.window_config import WindowConfiguration
from app.schemas import EventType


class ExperimentType(str, Enum):
    WINDOW_PERFORMANCE = "window_performance"
    WINDOW_CONFIGURATION_COMPARISON = "window_configuration_comparison"
    THROUGHPUT_BENCHMARK = "throughput_benchmark"
    RELIABILITY_TESTING = "reliability_testing"


class RunStatus(str, Enum):
    CREATED = "CREATED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class ExperimentWindowConfiguration(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    tumbling_size: int = 10
    sliding_size: int = 5
    sliding_interval: int = 1

    @model_validator(mode="after")
    def validate_window_configuration(self):
        try:
            WindowConfiguration(**self.model_dump())
        except (TypeError, ValueError) as exc:
            raise ValueError(str(exc)) from exc
        return self


class ExperimentWorkloadConfiguration(BaseModel):
    """Finite workload parameters snapshotted with each run."""

    model_config = ConfigDict(extra="forbid", strict=True)

    target_event_count: int = Field(default=100, ge=1, le=10000)
    publish_interval_ms: int = Field(default=100, ge=10, le=60000)
    event_type_distribution: Dict[str, int] = Field(
        default_factory=lambda: {event_type.value: 1 for event_type in EventType}
    )

    @model_validator(mode="after")
    def validate_distribution(self):
        if not self.event_type_distribution:
            raise ValueError("event_type_distribution must include a supported event type")
        if any(event_type not in {item.value for item in EventType}
               for event_type in self.event_type_distribution):
            raise ValueError("event_type_distribution contains an unsupported event type")
        if any(weight < 1 or weight > 1000 for weight in self.event_type_distribution.values()):
            raise ValueError("event type weights must be between 1 and 1000")
        return self


class ExperimentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=160)
    description: Optional[str] = Field(default=None, max_length=2000)
    experiment_type: ExperimentType = ExperimentType.WINDOW_PERFORMANCE
    default_configuration: ExperimentWindowConfiguration = Field(
        default_factory=ExperimentWindowConfiguration
    )
    default_workload_configuration: ExperimentWorkloadConfiguration = Field(
        default_factory=ExperimentWorkloadConfiguration
    )

    @field_validator("name")
    @classmethod
    def clean_name(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("name must not be blank")
        return value


class ExperimentUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: Optional[str] = Field(default=None, min_length=1, max_length=160)
    description: Optional[str] = Field(default=None, max_length=2000)
    experiment_type: Optional[ExperimentType] = None
    default_configuration: Optional[ExperimentWindowConfiguration] = None
    default_workload_configuration: Optional[ExperimentWorkloadConfiguration] = None

    @field_validator("name")
    @classmethod
    def clean_name(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        value = value.strip()
        if not value:
            raise ValueError("name must not be blank")
        return value

    @model_validator(mode="after")
    def require_update_field(self):
        if not self.model_fields_set:
            raise ValueError("at least one experiment field must be supplied")
        for field_name in ("name", "experiment_type"):
            if field_name in self.model_fields_set and getattr(self, field_name) is None:
                raise ValueError(f"{field_name} cannot be null")
        return self


class RunCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    run_id: Optional[str] = None
    configuration: Optional[ExperimentWindowConfiguration] = None
    workload_configuration: Optional[ExperimentWorkloadConfiguration] = None

    @field_validator("run_id")
    @classmethod
    def validate_run_id(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        try:
            return str(UUID(value))
        except (TypeError, ValueError, AttributeError) as exc:
            raise ValueError("run_id must be a valid UUID") from exc


class RunReason(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    reason: Optional[str] = Field(default=None, max_length=2000)

    @field_validator("reason")
    @classmethod
    def clean_reason(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        value = value.strip()
        if not value:
            raise ValueError("reason must not be blank when supplied")
        return value


class RunFailure(RunReason):
    reason: str = Field(min_length=1, max_length=2000)

