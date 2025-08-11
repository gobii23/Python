import csv
import os
from typing import List

BOOKS_FILE: str = "data/books.csv"


class Book:
    def __init__(self, title: str, author: str, isbn: str, available: bool = True) -> None:
        self.__title: str = title
        self.__author: str = author
        self.__isbn: str = isbn
        self.__available: bool = available

    def get_title(self) -> str:
        return self.__title

    def get_author(self) -> str:
        return self.__author

    def get_isbn(self) -> str:
        return self.__isbn

    def is_available(self) -> bool:
        return self.__available

    def set_available(self, status: bool) -> None:
        self.__available = bool(status)

    def to_list(self) -> List[str]:
        return [self.get_title(), self.get_author(), self.get_isbn(), str(self.is_available())]


class BookManager:
    @staticmethod
    def load_books() -> List[Book]:
        books: List[Book] = []
        if os.path.exists(BOOKS_FILE):
            with open(BOOKS_FILE, mode="r", newline="") as file:
                reader = csv.reader(file)
                next(reader, None)  
                for row in reader:
                    if row:
                        available = row[3] == "True"
                        books.append(Book(row[0], row[1], row[2], available))
        else:
            os.makedirs(os.path.dirname(BOOKS_FILE), exist_ok=True)
            with open(BOOKS_FILE, mode="w", newline="") as file:
                writer = csv.writer(file)
                writer.writerow(["title", "author", "isbn", "available"])
        return books

    @staticmethod
    def save_books(books: List[Book]) -> None:
        os.makedirs(os.path.dirname(BOOKS_FILE), exist_ok=True)
        with open(BOOKS_FILE, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["title", "author", "isbn", "available"])
            for book in books:
                writer.writerow(book.to_list())
