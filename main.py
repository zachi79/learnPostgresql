from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base, sessionmaker
import faker
from sqlalchemy import text

'''
Define PostgreSQL connection details
'''
DB_USER = "postgres"
DB_PASSWORD = "admin"
DB_HOST = "127.0.0.1"  # Change if using a remote DB
DB_PORT = "5432"  # Default PostgreSQL port
DB_NAME = "learn2"
TABLE_NAME = "learntable"


Base = declarative_base() # Create Base for ORM before defining models

class DatabaseConnection:
    """Class to manage database engine and session creation"""

    def __init__(self, db_url):
        self.engine = create_engine(db_url)
        self.SessionLocal = sessionmaker(bind=self.engine)

    def get_session(self):
        """Returns a new session"""
        return self.SessionLocal()


# Define ORM Model
class LearnTable(Base):
    __tablename__ = TABLE_NAME  # is a special attribute in SQLAlchemy that tells it the name of the table to which the model should be mapped.
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    email = Column(String, unique=True, nullable=False)
    age = Column(Integer, nullable=False)

    def __repr__(self):
        return f"<LearnTable(id={self.id}, name={self.name}, email={self.email}, age={self.age})>"


class DatabaseHandler:
    """Class to handle database operations"""

    def __init__(self, db_conn):
        self.session = db_conn.get_session()
        self.db_conn = db_conn

    def add_entry(self, name, email, age):
        """Add a row using ORM"""
        new_entry = LearnTable(name=name, email=email, age=age)
        self.session.add(new_entry)
        self.session.commit()
        print("Added:", new_entry)

    def add_entry_raw(self, name, email, age):
        """Add a row using raw SQL"""
        with self.db_conn.engine.connect() as conn:
            conn.execute(
                text("INSERT INTO learntable (name, email, age) VALUES (:name, :email, :age)"),
                {"name": name, "email": email, "age": age}
            )
            conn.commit()
        print("Added using raw SQL:", name, email, age)

    def get_all_entries(self):
        """Fetch all records"""
        return self.session.query(LearnTable).all()


def main():
    """Main function to execute database operations"""
    DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    db_connection = DatabaseConnection(DATABASE_URL) # Initialize Database Connection

    Base.metadata.create_all(db_connection.engine) # Create the table -  Ensures Table Existence (Avoids Errors)
    db_handler = DatabaseHandler(db_connection) # Initialize Database Handler

    fake = faker.Faker()  # Generate a random name
    random_name = fake.name() # Generate a random email
    random_email = fake.email()
    random_age = fake.random_int(min=18, max=60)
    random_name2 = fake.name()
    random_email2 = fake.email()
    random_age2 = fake.random_int(min=1, max=100)

    db_handler.add_entry(random_name, random_email, random_age) # Adding entries
    db_handler.add_entry_raw(random_name2, random_email2, random_age2) # Adding entries

    print("\nAll Records in 'learnTable':")
    for row in db_handler.get_all_entries(): # Fetch and print all records
        print(row)


if __name__ == "__main__":
    main()
