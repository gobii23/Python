from abc import ABC, abstractmethod
from datetime import datetime

class LateFeeStrategy(ABC):
    @abstractmethod
    def calculate_fee(self, due_date: str, return_date: str) -> float:
        pass


class StudentLateFee(LateFeeStrategy):
    def calculate_fee(self, due_date: str, return_date: str) -> float:
        due = datetime.strptime(due_date, "%Y-%m-%d")
        ret = datetime.strptime(return_date, "%Y-%m-%d")
        days_late = (ret - due).days
        return max(0, days_late * 2.0) 


class FacultyLateFee(LateFeeStrategy):
    def calculate_fee(self, due_date: str, return_date: str) -> float:
        due = datetime.strptime(due_date, "%Y-%m-%d")
        ret = datetime.strptime(return_date, "%Y-%m-%d")
        days_late = (ret - due).days
        return max(0, days_late * 1.0) 
