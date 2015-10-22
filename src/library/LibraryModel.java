package library;

/*
 * LibraryModel.java
 * Author:
 * Created on:
 */

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import javax.swing.*;

public class LibraryModel {

	// For use in creating dialogs and making them modal
	private JFrame dialogParent;
	private Connection con;

	public LibraryModel(JFrame parent, String userid, String password) {
		initConnection(userid, password);
	}

	/**
	 * Sets a rollback point
	 */
	private boolean setRollback() {
		try {
			System.out.println("Attempting to set a rollback point...");
			con.setAutoCommit(false);
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		System.out.println("Rollback point set.");
		return true;
	}

	/**
	 * Commits a change
	 */
	private boolean commit() {
		try {
			System.out.println("Attempting to commit...");
			con.commit();
			con.setAutoCommit(true);
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		System.out.println("Changes commited.");
		return true;
	}

	/**
	 * Rolls back to the previous point
	 */
	private boolean rollback() {
		try {
			System.out.println("Attempting to rollback...");
			con.rollback();
			con.setAutoCommit(true);
		} catch (SQLException e) {
			// e.printStackTrace();
			return false;
		}
		System.out.println("Rollback successful");
		return true;
	}

	/**
	 * Attempts to connect to the database with the given userid and password
	 *
	 * @param userid
	 * @param password
	 */
	private void initConnection(String userid, String password) {
		try {
			con = DriverManager.getConnection(
					"jdbc:postgresql://db.ecs.vuw.ac.nz/" + userid + "_jdbc",
					userid, password);
		} catch (SQLException e) {
			e.printStackTrace();
			closeDBConnection();
		}
	}

	/**
	 * Returns a string that references the book with the given isbn
	 *
	 * @param isbn
	 * @return
	 */
	public String bookLookup(int isbn) {
		if (isbn < 0)
			return invalidInput("isbn", isbn + "");
		StringBuilder sb = new StringBuilder();
		sb.append("Book Lookup:\n\t");

		sb.append(lookup(isbn));

		sb.append("\n");
		return sb.toString();
	}

	/**
	 * Generates a string for a book
	 *
	 * @param isbn
	 * @return
	 */
	private String lookup(int isbn) {
		if (isbn < 0)
			return invalidInput("isbn", isbn + "");
		String query = "SELECT Book.Title, Book.Edition_No, Book.NumOfCop, Book.NumLeft, "
				+ "Author.AuthorId, Author.Surname "
				+ "FROM Book "
				+ "LEFT OUTER JOIN Book_Author ON Book.ISBN = Book_Author.ISBN "
				+ "LEFT OUTER JOIN Author ON Book_Author.AuthorId = Author.AuthorId "
				+ "WHERE Book.ISBN = " + isbn + " ORDER BY AuthorSeqNo ASC;";
		Statement s = null;
		ResultSet rs = null;
		try {
			s = con.createStatement();
			rs = s.executeQuery(query);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		if (s == null || rs == null)
			return "Invalid Query, Something went wrong.";
		StringBuilder sb = new StringBuilder();
		sb.append(isbn + ": ");

		boolean firstTuple = true;

		try {
			while (rs.next()) {
				if (firstTuple) {
					sb.append(rs.getString("Title").trim() + "\n\t");
					sb.append("Edition: " + rs.getString("Edition_No"));
					sb.append(" - Number of copies: "
							+ rs.getString("NumOfCop"));
					sb.append(" - Copies left: " + rs.getString("NumLeft")
							+ "\n\t");
					sb.append("Authors: ");
				}

				if (!firstTuple) {
					sb.append(", ");
				}

				String surname = rs.getString("Surname");
				if (surname == null) {
					sb.append("(No Authors)");
				} else {
					sb.append(surname.trim());
				}
				firstTuple = false;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		// sb.append("\n");
		return sb.toString();
	}

	/**
	 * Given an isbn for a book, returns all the current borrowers of that book
	 *
	 * @param isbn
	 * @return
	 */
	private List<String> findBorrowers(int isbn) {
		List<String> borrowers = new ArrayList<String>();
		try {

			String query = "SELECT CustomerId, L_Name, F_Name, City"
					+ " FROM Customer" + " NATURAL JOIN Cust_Book"
					+ " NATURAL JOIN Book" + " WHERE ISBN = " + isbn + ";";

			Statement s = con.createStatement();
			ResultSet rs = s.executeQuery(query);

			while (rs.next()) {
				StringBuilder sb = new StringBuilder();
				String id = rs.getString("CustomerID").trim();
				String lastName = rs.getString("L_Name").trim();
				String firstName = rs.getString("F_Name").trim();
				String city = rs.getString("City").trim();

				sb.append(id + ": ");
				sb.append(lastName + ", ");
				sb.append(firstName + " - ");
				sb.append(city);

				borrowers.add(sb.toString());
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}

		return borrowers;
	}

	/**
	 * Returns the entire catalogue of books
	 *
	 * @return
	 */
	public String showCatalogue() {
		StringBuilder sb = new StringBuilder();

		try {

			sb.append("Entire Catalogue:\n");
			String query = "SELECT ISBN FROM Book ORDER BY ISBN";

			Statement s = null;
			s = con.createStatement();
			ResultSet rs = s.executeQuery(query);

			while (rs.next()) {
				String isbn = rs.getString("ISBN");
				sb.append(lookup(Integer.parseInt(isbn)) + "\n");
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}

		return sb.toString();
	}

	/**
	 * Returns the list of books that are loaned
	 *
	 * @return
	 */
	public String showLoanedBooks() {
		StringBuilder sb = new StringBuilder();
		sb.append("Show Loaned Books:\n\n");

		String query = "SELECT DISTINCT ISBN FROM Cust_Book";

		try {
			Statement s = con.createStatement();
			ResultSet rs;
			rs = s.executeQuery(query);

			boolean hasBooks = false;

			while (rs.next()) {
				hasBooks = true;
				String isbn = rs.getString("ISBN");
				sb.append(lookup(Integer.parseInt(isbn)) + "\n");

				List<String> borrowers = findBorrowers(Integer.parseInt(isbn));
				for (String borrower : borrowers) {
					sb.append("\t\t" + borrower + "\n");
				}
			}

			if (!hasBooks) {
				sb.append("\tNo borrowed books");
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}

		return sb.toString();
	}

	/**
	 * Given the authorID returns a string with info about the author
	 *
	 * @param authorID
	 * @return
	 */
	public String showAuthor(int authorID) {
		if (authorID < 0)
			return invalidInput("authorID", authorID + "");

		StringBuilder sb = new StringBuilder();
		sb.append("Show Author:\n");

		// Print the name
		sb.append(author(authorID));

		// Print the books
		sb.append("\n\tBooks written:\n");
		List<String> books = booksByAuthor(authorID);

		if (books.isEmpty()) {
			sb.append("\t\t(No books authored)");
			return sb.toString();
		}

		for (String book : books) {
			sb.append("\t\t" + book + "\n");
		}

		return sb.toString();
	}

	/**
	 * Returns a list of string that references all the books written by the
	 * give author
	 *
	 * @param authorId
	 * @return
	 */
	private List<String> booksByAuthor(int authorId) {
		List<String> books = new ArrayList<String>();

		try {

			String query = "SELECT ISBN, Title" + " FROM Book_Author"
					+ " NATURAL JOIN Book" + " WHERE AuthorId = " + authorId
					+ "ORDER BY ISBN;";
			Statement s = con.createStatement();
			ResultSet rs = s.executeQuery(query);

			while (rs.next()) {

				StringBuilder sb = new StringBuilder();
				String isbn = rs.getString("ISBN");
				String title = rs.getString("Title");

				sb.append(isbn + " - ");
				sb.append(title);

				books.add(sb.toString());
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}

		return books;
	}

	/**
	 * Generates and returns the author with the authorID
	 *
	 * @param authorId
	 * @return
	 */
	private String author(int authorId) {
		StringBuilder sb = new StringBuilder();

		try {
			if (authorId < 0)
				return invalidInput("authorId", authorId + "");
			String query = "SELECT AuthorId, Name, Surname" + " FROM Author"
					+ " WHERE AuthorId = " + authorId + ";";
			Statement s = null;
			ResultSet rs = null;
			s = con.createStatement();
			rs = s.executeQuery(query);
			if (s == null || rs == null)
				return "Invalid Query, Something went wrong.";

			while (rs.next()) {
				String id = rs.getString("AuthorId").trim();
				String fName = rs.getString("Name").trim();
				String lName = rs.getString("Surname").trim();

				sb.append("\t");
				sb.append(id + ": ");
				sb.append(lName + ", ");
				sb.append(fName);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return sb.toString();
	}

	/**
	 * Returns a string containing all the authors in the database
	 *
	 * @return
	 */
	public String showAllAuthors() {
		StringBuilder sb = new StringBuilder();

		try {

			sb.append("Show All Authors:\n");
			String query = "SELECT AuthorId FROM Author ORDER BY AuthorId";

			Statement s = con.createStatement();
			ResultSet rs = s.executeQuery(query);

			while (rs.next()) {
				String id = rs.getString("AuthorId");
				sb.append("\t" + author(Integer.parseInt(id)) + "\n");
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}

		return sb.toString();
	}

	/**
	 * Returns a string of the customer with the given customerID
	 *
	 * @param customerID
	 * @return
	 */
	public String showCustomer(int customerID) {
		if (customerID < 0)
			return invalidInput("customerID", customerID + "");

		StringBuilder sb = new StringBuilder();
		sb.append("Show Customer:\n");

		// Print the name
		sb.append(customer(customerID));

		// Print the books
		sb.append("\tBooks borrowed:\n");
		List<String> books = booksByCustomer(customerID);

		if (books.isEmpty()) {
			sb.append("\t\t(Not currently loaning any books)");
			return sb.toString();
		}

		for (String book : books) {
			sb.append("\t\t" + book + "\n");
		}

		return sb.toString();
	}

	/**
	 * Returns a list of string with all the boaks currently loaned by the
	 * customer
	 *
	 * @param customerId
	 * @return
	 */
	private List<String> booksByCustomer(int customerId) {
		List<String> books = new ArrayList<String>();

		try {

			String query = "SELECT ISBN, Title" + " FROM Cust_Book"
					+ " NATURAL JOIN Book" + " WHERE CustomerId = "
					+ customerId + "ORDER BY ISBN;";
			Statement s = con.createStatement();
			ResultSet rs = s.executeQuery(query);

			while (rs.next()) {

				StringBuilder sb = new StringBuilder();
				String isbn = rs.getString("ISBN");
				String title = rs.getString("Title");

				sb.append(isbn + " - ");
				sb.append(title);

				books.add(sb.toString());
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}

		return books;
	}

	/**
	 * Generates the base customer string
	 *
	 * @param customerId
	 * @return
	 */
	private String customer(int customerId) {
		StringBuilder sb = new StringBuilder();
		try {
			if (customerId < 0)
				return invalidInput("customerId", customerId + "");

			String query = "SELECT L_Name, F_Name, City" + " FROM Customer"
					+ " WHERE CustomerID = " + customerId + ";";
			Statement s = con.createStatement();
			ResultSet rs = s.executeQuery(query);

			if (s == null || rs == null)
				return "Invalid Query, Something went wrong.";

			while (rs.next()) {
				String id = customerId + "";
				String lName = rs.getString("L_Name").trim();
				String fName = rs.getString("F_Name").trim();
				String city = rs.getString("City");

				sb.append("\t" + id + ": ");
				sb.append(lName + ", ");
				sb.append(fName + " - ");

				if (city == null) {
					sb.append("(No city)");
				} else {
					sb.append(city.trim());
				}

				sb.append("\n");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return sb.toString();
	}

	/**
	 * Returns a string containing all the customers in the database
	 *
	 * @return
	 */
	public String showAllCustomers() {
		StringBuilder sb = new StringBuilder();

		try {

			sb.append("Show All Customers:\n");
			String query = "SELECT CustomerId FROM Customer ORDER BY CustomerId";

			Statement s = con.createStatement();
			ResultSet rs = s.executeQuery(query);

			while (rs.next()) {
				String id = rs.getString("CustomerId");
				sb.append(customer(Integer.parseInt(id)));
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}

		return sb.toString();
	}

	/**
	 * Borrows the book with the given information
	 *
	 * @param isbn
	 * @param customerID
	 * @param day
	 * @param month
	 * @param year
	 * @return
	 */
	public String borrowBook(int isbn, int customerID, int day, int month,
			int year) {

		// Set the month to start at index 1
		month++;

		if (setRollback()) {
			try {
				StringBuilder sb = new StringBuilder();

				// First get Book name and lock book
				String bookTitleQuery = "SELECT Title, NumLeft" + " FROM Book"
						+ " WHERE ISBN = " + isbn + " FOR UPDATE;";
				Statement bookS = con.createStatement();
				ResultSet bookRs = bookS.executeQuery(bookTitleQuery);
				String title = null;
				if (bookRs.next()) {
					title = bookRs.getString("Title").trim();
					if (Integer.parseInt(bookRs.getString("NumLeft")) <= 0) {
						return "No copies left of " + title;
					}
				} else {
					rollback();
					return "Book ISBN " + isbn
							+ " does not exist, made no changes";
				}

				// Second get Customer name and lock customer
				String custQuery = "SELECT F_Name, L_Name" + " FROM Customer"
						+ " WHERE CustomerId = " + customerID + " FOR UPDATE;";
				Statement custS = con.createStatement();
				ResultSet custRs = custS.executeQuery(custQuery);
				String name = null;
				if (custRs.next()) {
					name = custRs.getString("F_Name").trim() + " "
							+ custRs.getString("L_Name").trim();
				} else {
					rollback();
					return "Customer with ID " + customerID
							+ " does not exist, made no changes";
				}

				// Thirdly check to see if the customer has already got the book
				// checked out
				String dupeQuery = "SELECT *" + " FROM Cust_Book"
						+ " WHERE ISBN = " + isbn + " AND CustomerId = "
						+ customerID + ";";
				Statement dupeS = con.createStatement();
				ResultSet dupeRs = dupeS.executeQuery(dupeQuery);
				if (dupeRs.next()) {
					rollback();
					return "Customer already has book checked out.";
				}

				// Query for creating new entry in cust_book table
				String custBookQuery = "INSERT INTO Cust_Book (CustomerId, DueDate, ISBN)"
						+ " VALUES ("
						+ customerID
						+ ", DATE '"
						+ year
						+ "-"
						+ month + "-" + day + "'," + isbn + ")";

				// Query for lowering the amount of copies of the book available
				// by one
				String bookLoanQuery = "UPDATE Book"
						+ " SET NumLeft = NumLeft-1" + " WHERE ISBN = " + isbn
						+ ";";

				Statement custBookS = con.createStatement();
				Statement bookLoanS = con.createStatement();
				custBookS.executeUpdate(custBookQuery);
				bookLoanS.executeUpdate(bookLoanQuery);

				// Construct the string
				sb.append("Borrow Book:\n");
				sb.append("\tBook: " + isbn + "(" + title + ")\n");
				sb.append("\tLoaned to: " + customerID + "(" + name + ")\n");
				sb.append("\tDue Date: " + day + "/" + month + "/" + year);

				String message = "Locked the tuples, ready to update. Do you want to continue?";

				if (JOptionPane.showConfirmDialog(dialogParent, message,
						"Confirm choice", JOptionPane.YES_NO_OPTION) == JOptionPane.YES_OPTION) {
					commit();
					return sb.toString();
				} else {
					rollback();
					return "Made no changes.";
				}

			} catch (SQLException e) {
				rollback();
				e.printStackTrace();
				return "An exception occured";
			}
		}

		return "Could not set a rollback point, this operation will not continue";
	}

	/**
	 * Returns the book with the given information
	 *
	 * @param isbn
	 * @param customerid
	 * @return
	 */
	public String returnBook(int isbn, int customerid) {
		if (setRollback()) {
			try {

				ResultSet rs;

				// First find and lock the customer
				String custQuery = "SELECT *" + " FROM Customer"
						+ " WHERE CustomerId = " + customerid + " FOR UPDATE";
				Statement custS = con.createStatement();
				rs = custS.executeQuery(custQuery);
				if (!rs.next()) {
					rollback();
					return "Customer " + customerid + " does not exist.";
				}

				// Second find and lock the book
				String bookQuery = "SELECT *" + " FROM Book" + " WHERE ISBN = "
						+ isbn + " FOR UPDATE";
				Statement bookS = con.createStatement();
				rs = bookS.executeQuery(bookQuery);
				if (!rs.next()) {
					rollback();
					return "Book " + isbn + " does not exist.";
				}

				// Third find and lock the cust_book entry
				String custBookQuery = "SELECT *" + " FROM Cust_Book"
						+ " WHERE CustomerId = " + customerid + " AND ISBN = "
						+ isbn + " FOR UPDATE";
				Statement custBookS = con.createStatement();
				rs = custBookS.executeQuery(custBookQuery);
				if (!rs.next()) {
					rollback();
					return "Customer " + customerid + " is not borrowing book "
							+ isbn + ".";
				}

				// Execute the code to delete the cust_book entry
				String delQuery = "DELETE FROM Cust_Book"
						+ " WHERE CustomerId = " + customerid + " AND ISBN = "
						+ isbn;
				Statement delS = con.createStatement();
				delS.executeUpdate(delQuery);

				// Increase the amount of copies of the book left
				String bookLoanQuery = "UPDATE Book"
						+ " SET NumLeft = NumLeft+1" + " WHERE ISBN = " + isbn
						+ ";";
				Statement bookLoanS = con.createStatement();
				bookLoanS.executeUpdate(bookLoanQuery);

				// Commit the changes
				commit();

				StringBuilder sb = new StringBuilder();
				sb.append("Return Book:\n\t");
				sb.append("Book " + isbn + " returned for customer "
						+ customerid);

				return sb.toString();

			} catch (SQLException e) {
				// Rollback any changes that may have occured
				rollback();
				e.printStackTrace();
				return "An exception occured";
			}
		}
		return "Could not set a rollback point, this operation will not continue";
	}

	/**
	 * Closes the current connection
	 */
	public void closeDBConnection() {

		System.out.println("Closing database connection...");

		try {
			if (con == null || con.isClosed())
				return;

			try {
				System.out.println("Attempting to rollback...");
				con.rollback();
				con.setAutoCommit(true);
			} catch (SQLException e) {
				System.out.println("Nothing to rollback.");
			}

			// Close the connection
			con.close();

			System.out.println("Database connection successfully closed");
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Error closing connection");
		}

		System.out.println("The program will now exit.");
		System.exit(0);
	}

	public String deleteCus(int customerID) {
		if (customerID < 0)
			return invalidInput("customerID", customerID + "");
		return "Delete Customer";
	}

	public String deleteAuthor(int authorID) {
		if (authorID < 0)
			return invalidInput("authorID", authorID + "");
		return "Delete Author";
	}

	public String deleteBook(int isbn) {
		if (isbn < 0)
			return invalidInput("isbn", isbn + "");
		return "Delete Book";
	}

	/**
	 * Generates and returns an invalid input string
	 *
	 * @param name
	 * @param input
	 * @return
	 */
	private String invalidInput(String name, String input) {
		return "Invalid " + name + ". " + name + " was: " + input;
	}

}
