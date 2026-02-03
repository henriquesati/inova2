from dataclasses import dataclass
from typing import Generic, TypeVar, Callable, Optional

T = TypeVar("T")
E = TypeVar("E")


@dataclass(frozen=True)
class Result(Generic[T]):
    _value: Optional[T] = None
    _error: Optional[E] = None

    @property
    def is_ok(self) -> bool:
        return self._error is None

    @property
    def is_err(self) -> bool:
        return self._error is not None

    @property
    def value(self) -> T:
        if self.is_err:
            raise RuntimeError("Tentativa de acessar value de um Result em erro")
        return self._value  # type: ignore

    @property
    def error(self) -> E:
        if self.is_ok:
            raise RuntimeError("Tentativa de acessar error de um Result em sucesso")
        return self._error  # type: ignore

    @staticmethod
    def ok(value: T) -> "Result[T]":
        return Result(_value=value)

    @staticmethod
    def err(error: E) -> "Result[T]":
        return Result(_error=error)

    def bind(self, fn: Callable[[T], "Result[T]"]) -> "Result[T]":
        if self.is_err:
            return self
        return fn(self._value)  # type: ignore
    
    def map(self, fn):
        if self.is_err:
            return self
        try:
            return Result.ok(fn(self._value))
        except Exception as e:
            return Result.err(str(e))