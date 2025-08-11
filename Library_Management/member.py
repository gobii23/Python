import csv
import os
from typing import List, Tuple
from fee import LateFeeStrategy, StudentLateFee, FacultyLateFee

MEMBERS_FILE: str = "data/members.csv"


class Member:
    def __init__(self, name: str, member_id: str, member_type: str) -> None:
        self.__name: str = name
        self.__member_id: str = member_id
        self.__member_type: str = member_type
        self.__borrowed_books: List[Tuple[str, str]] = []
        self.late_fee_strategy: LateFeeStrategy

    def get_name(self) -> str:
        return self.__name

    def get_member_id(self) -> str:
        return self.__member_id

    def get_member_type(self) -> str:
        return self.__member_type

    def get_borrowed_books(self) -> List[Tuple[str, str]]:
        return list(self.__borrowed_books)

    def add_borrowed_book(self, title: str, return_date: str) -> None:
        self.__borrowed_books.append((title, return_date))

    def remove_borrowed_book(self, title: str) -> None:
        self.__borrowed_books = [(t, d) for (t, d) in self.__borrowed_books if t != title]

    def to_list(self) -> List[str]:
        borrowed_str: str = ";".join([f"{title}|{date}" for title, date in self.__borrowed_books])
        return [self.get_name(), self.get_member_id(), self.get_member_type(), borrowed_str]

    def borrow_period(self) -> int:
        return 0

    def calculate_late_fee(self, due_date: str, return_date: str) -> float:
        return self.late_fee_strategy.calculate_fee(due_date, return_date)


class StudentMember(Member):
    def __init__(self, name: str, member_id: str, member_type: str = "Student") -> None:
        super().__init__(name, member_id, member_type)
        self.late_fee_strategy = StudentLateFee()

    def borrow_period(self) -> int:
        return 14 


class FacultyMember(Member):
    def __init__(self, name: str, member_id: str, member_type: str = "Faculty") -> None:
        super().__init__(name, member_id, member_type)
        self.late_fee_strategy = FacultyLateFee()

    def borrow_period(self) -> int:
        return 28 


class MemberManager:
    @staticmethod
    def load_members() -> List[Member]:
        members: List[Member] = []
        if os.path.exists(MEMBERS_FILE):
            with open(MEMBERS_FILE, mode="r", newline="") as file:
                reader = csv.reader(file)
                next(reader, None)
                for row in reader:
                    if row:
                        if row[2] == "Student":
                            member = StudentMember(row[0], row[1], row[2])
                        else:
                            member = FacultyMember(row[0], row[1], row[2])
                        if row[3]:
                            for entry in row[3].split(";"):
                                if "|" in entry:
                                    title, date = entry.split("|", 1)
                                    member.add_borrowed_book(title, date)
                        members.append(member)
        else:
            os.makedirs(os.path.dirname(MEMBERS_FILE), exist_ok=True)
            with open(MEMBERS_FILE, mode="w", newline="") as file:
                writer = csv.writer(file)
                writer.writerow(["name", "member_id", "member_type", "borrowed_books"])
        return members

    @staticmethod
    def save_members(members: List[Member]) -> None:
        os.makedirs(os.path.dirname(MEMBERS_FILE), exist_ok=True)
        with open(MEMBERS_FILE, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["name", "member_id", "member_type", "borrowed_books"])
            for member in members:
                writer.writerow(member.to_list())
