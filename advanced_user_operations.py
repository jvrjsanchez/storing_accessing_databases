import sqlite3

class AdvancedUserOperations:

    def __init__(self):
        self.conn = sqlite3.connect('user_database.db')
        self.cursor = self.conn.cursor()
        self.setup_table()

    def setup_table(self):
        # Creating the users table if it doesn't exist
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT,
            email TEXT UNIQUE,
            password TEXT,
            age INTEGER,
            gender TEXT,
            address TEXT
        )''')
        self.conn.commit()

    def create_user_with_profile(self, name, email, password, age=None, gender=None, address=None):
        try:
            self.cursor.execute('''INSERT INTO users (name, email, password, age, gender, address)
                                   VALUES (?, ?, ?, ?, ?, ?)''', 
                                   (name, email, password, age, gender, address))
            self.conn.commit()
            return "User created successfully."
        except sqlite3.IntegrityError:
            return "A user with this email already exists."

    def retrieve_users_by_criteria(self, min_age=None, max_age=None, gender=None):
        query = "SELECT * FROM users WHERE 1=1"
        params = []
        if min_age is not None:
            query += " AND age >= ?"
            params.append(min_age)
        if max_age is not None:
            query += " AND age <= ?"
            params.append(max_age)
        if gender:
            query += " AND gender = ?"
            params.append(gender)
        self.cursor.execute(query, params)
        return self.cursor.fetchall()

    def update_user_profile(self, email, age=None, gender=None, address=None):
        fields = []
        params = []
        if age is not None:
            fields.append("age = ?")
            params.append(age)
        if gender is not None:
            fields.append("gender = ?")
            params.append(gender)
        if address is not None:
            fields.append("address = ?")
            params.append(address)
        params.append(email)
        update_statement = f"UPDATE users SET {', '.join(fields)} WHERE email = ?"
        self.cursor.execute(update_statement, params)
        self.conn.commit()
        return "User profile updated successfully."

    def delete_users_by_criteria(self, gender=None):
        if gender:
            self.cursor.execute("DELETE FROM users WHERE gender = ?", (gender,))
            self.conn.commit()
            return "Users deleted successfully."
        else:
            return "No criteria given for deletion."

    def __del__(self):
        self.conn.close()
