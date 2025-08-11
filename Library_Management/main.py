from library import Library
from member import StudentMember, FacultyMember, Member

ADMIN_PASSWORD: str = "admin123"


def admin_menu(library: Library) -> None:
    while True:
        print("\n--- Admin Menu ---")
        print("1. Add Book")
        print("2. Delete Book")
        print("3. Add Member")
        print("4. Delete Member")
        print("5. View Borrowed Books Report")
        print("6. Display Available Books")
        print("7. View All Members")
        print("8. Logout")
        choice = input("Enter choice: ")

        if choice == "1":
            title = input("Enter book title: ")
            author = input("Enter author: ")
            isbn = input("Enter ISBN: ")
            library.add_book(title, author, isbn)
        elif choice == "2":
            isbn = input("Enter ISBN of book to delete: ")
            library.delete_book(isbn)
        elif choice == "3":
            name = input("Enter member name: ")
            member_id = input("Enter member ID: ")
            member_type = input("Enter type (student/faculty): ").lower()
            if member_type == "student":
                member: Member = StudentMember(name, member_id, "Student")
            elif member_type == "faculty":
                member = FacultyMember(name, member_id, "Faculty")
            else:
                print("Invalid member type.")
                continue
            library.add_member(member)
        elif choice == "4":
            member_id = input("Enter member ID to delete: ")
            library.delete_member(member_id)
        elif choice == "5":
            library.view_borrowed_books()
        elif choice == "6":
            library.display_available_books()
        elif choice == "7":
            library.view_all_members()
        elif choice == "8":
            break
        else:
            print("Invalid choice.")


def student_faculty_menu(library: Library, member_id: str, role: str) -> None:
    while True:
        print(f"\n--- {role} Menu ---")
        print("1. Borrow Book")
        print("2. Return Book")
        print("3. Display Available Books")
        print("4. View My Borrowed Books")
        print("5. Logout")
        choice: str = input("Enter choice: ")

        if choice == "1":
            isbn = input("Enter ISBN to borrow: ")
            library.borrow_book(member_id, isbn)
        elif choice == "2":
            isbn = input("Enter ISBN to return: ")
            library.return_book(member_id, isbn)
        elif choice == "3":
            library.display_available_books()
        elif choice == "4":
            library.view_borrowed_books_by_member(member_id)
        elif choice == "5":
            break
        else:
            print("Invalid choice.")


def main() -> None:
    library = Library("My Library")

    while True:
        print("\nLogin as:")
        print("1. Admin")
        print("2. Student")
        print("3. Faculty")
        print("4. Exit")
        choice: str = input("Enter choice: ")

        if choice == "1":
            password = input("Enter admin password: ")
            if password == ADMIN_PASSWORD:
                admin_menu(library)
            else:
                print("Incorrect password.")
        elif choice == "2":
            member_id = input("Enter your Member ID: ")
            student_faculty_menu(library, member_id, "Student")
        elif choice == "3":
            member_id = input("Enter your Member ID: ")
            student_faculty_menu(library, member_id, "Faculty")
        elif choice == "4":
            print("Goodbye!")
            break
        else:
            print("Invalid choice.")


if __name__ == "__main__":
    main()
