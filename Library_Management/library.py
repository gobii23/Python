from datetime import datetime, timedelta
from typing import List
from book import Book, BookManager
from member import Member, MemberManager

class Library:
    def __init__(self, name: str) -> None:
        self.name: str = name
        self.books: List[Book] = BookManager.load_books()
        self.members: List[Member] = MemberManager.load_members()

    def add_book(self, title: str, author: str, isbn: str) -> None:
        self.books.append(Book(title, author, isbn))
        BookManager.save_books(self.books)
        print("Book added successfully.")

    def delete_book(self, isbn: str) -> None:
        self.books = [book for book in self.books if book.get_isbn() != isbn]
        BookManager.save_books(self.books)
        print("Book deleted successfully.")

    def add_member(self, member: Member) -> None:
        self.members.append(member)
        MemberManager.save_members(self.members)
        print("Member added successfully.")

    def delete_member(self, member_id: str) -> None:
        self.members = [m for m in self.members if m.get_member_id() != member_id]
        MemberManager.save_members(self.members)
        print("Member deleted successfully.")

    def borrow_book(self, member_id: str, isbn: str) -> None:
        member = next((m for m in self.members if m.get_member_id() == member_id), None)
        book = next((b for b in self.books if b.get_isbn() == isbn and b.is_available()), None)
        if member and book:
            book.set_available(False)
            return_date = (datetime.now() + timedelta(days=member.borrow_period())).strftime("%Y-%m-%d")
            member.add_borrowed_book(book.get_title(), return_date)
            BookManager.save_books(self.books)
            MemberManager.save_members(self.members)
            print(f"Book borrowed. Return by {return_date}")
        else:
            print("Book not available or member not found.")

    def return_book(self, member_id: str, isbn: str) -> None:
        member = next((m for m in self.members if m.get_member_id() == member_id), None)
        book = next((b for b in self.books if b.get_isbn() == isbn), None)
        if member and book:
            return_date = datetime.now().strftime("%Y-%m-%d")
            for title, due_date in member.get_borrowed_books():
                if title == book.get_title():
                    fee = member.calculate_late_fee(due_date, return_date)
                    if fee > 0:
                        print(f" Late fee: ₹{fee:.2f}")
            member.remove_borrowed_book(book.get_title())
            book.set_available(True)
            BookManager.save_books(self.books)
            MemberManager.save_members(self.members)
            print("Book returned successfully.")
        else:
            print("Member or book not found.")

    def display_available_books(self) -> None:
        print("\n Available Books:")
        for book in self.books:
            if book.is_available():
                print(f"{book.get_title()} by {book.get_author()} (ISBN: {book.get_isbn()})")

    def view_all_members(self) -> None:
        print("\nMembers List:")
        for member in self.members:
            borrowed_info = ", ".join([f"{title} (Return: {date})" for title, date in member.get_borrowed_books()]) or "No books borrowed"
            print(f"{member.get_name()} | {member.get_member_id()} | {member.get_member_type()} | {borrowed_info}")

    def view_borrowed_books(self) -> None:
        print("\nBorrowed Books Report:")
        any_borrowed = False
        for member in self.members:
            for title, date in member.get_borrowed_books():
                print(f"{title} borrowed by {member.get_name()} (ID: {member.get_member_id()}) - Return by {date}")
                any_borrowed = True
        if not any_borrowed:
            print("No books currently borrowed.")

    def view_borrowed_books_by_member(self, member_id: str) -> None:
        member = next((m for m in self.members if m.get_member_id() == member_id), None)
        if member:
            borrowed = member.get_borrowed_books()
            if borrowed:
                print(f"\n Books borrowed by {member.get_name()}:")
                for title, return_date in borrowed:
                    print(f"🔸 {title} (Return by: {return_date})")
            else:
                print("\nYou haven't borrowed any books.")
        else:
            print("Member not found.")
