# -*- coding: utf-8 -*-
"""
Created on Wed Oct 29 21:40:00 2025

@author: mgdin
"""
import os
os.chdir(r'C:\Users\mgdin\OneDrive\Documents\dev\TRG_App\dev\trgapp')

from trg_config import config


from flask import Flask
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
db = SQLAlchemy()

sqlite_file_path = config['sqlite_db']
DATABASE_URI=f'sqlite:///{sqlite_file_path}'
app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URI

# Bind SQLAlchemy to the API app within the application context
with app.app_context():
    db.init_app(app)

def test_user():
    from database import models
    app.app_context().push()
    users = models.User.query.all()

    username = 'test@trg.com'
    user = models.User.query.filter_by(username=username).first()
    print(user)




models.Users


#################################################################
# 
def get_pg_url():
    dbname = 'postgres'        
    user = os.environ["PRS_USERNAME"]     
    password = os.environ["PRS_PASSWORD"]
    host = 'trg-input-database.c9sm826ie6uy.us-east-1.rds.amazonaws.com'
    port = '5432'
    POSTGRESQL_URI = f'postgresql://{user}:{password}@{host}:{port}/{dbname}'
    return POSTGRESQL_URI 

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# --- Load Environment Variables ---
# In development, load from .env file. 
# In production, variables are typically set directly on the server.
load_dotenv()

# --- Configuration Logic ---

# 1. Determine the environment (default to 'development')
APP_ENV = os.getenv("APP_ENV", "development").lower()

if APP_ENV == "production":
    # Use PostgreSQL connection string from environment variables
    DATABASE_URL = os.getenv("PROD_DATABASE_URL")
    if not DATABASE_URL:
        raise ValueError("PROD_DATABASE_URL not set for production environment!")
    print("Using PostgreSQL Engine (Production mode)")
    # Disable check_same_thread for SQLAlchemy 1.4+ and 2.0+ 
    # when using SQLite, but for PostgreSQL, we don't need it.

elif APP_ENV == "development":
    # Use SQLite connection string
    DATABASE_URL = os.getenv("DEV_DATABASE_URL", "sqlite:///./dev_database.sqlite3")
    print("Using SQLite Engine (Development mode)")

else:
    raise ValueError(f"Unknown environment: {APP_ENV}")

# 2. Create the SQLAlchemy Engine
# The 'echo=True' prints all SQL commands to the console (great for debugging in dev)
engine = create_engine(
    DATABASE_URL, 
    echo=True if APP_ENV == "development" else False,
    connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {}
)

# 3. Create a Session and Base
# Base is used to define all ORM classes
Base = declarative_base()

# Session is the primary way to interact with the database
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# --- Dependency Function for Application Use ---

def get_db():
    """
    A simple function (often used in web frameworks) to get a database session.
    It ensures the session is closed after use.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- Example of defining a model (ORM) ---
from sqlalchemy import Column, Integer, String

class Item(Base):
    __tablename__ = "items"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(50), nullable=False)
    description = Column(String)

# Function to create tables (will work for both DBs)
def init_db():
    # Only creates tables if they don't exist
    Base.metadata.create_all(bind=engine)
    print(f"Database tables created for {APP_ENV}.")

# --- Example Usage ---
if __name__ == "__main__":
    init_db()

    # Get a database session
    db_session_generator = get_db()
    db = next(db_session_generator) # Get the session object

    # Add a new item
    new_item = Item(name=f"Test Item in {APP_ENV}", description="This works everywhere!")
    db.add(new_item)
    db.commit()

    # Query all items
    items = db.query(Item).all()
    print("\n--- Current Items ---")
    for item in items:
        print(f"ID: {item.id}, Name: {item.name}")
    print("---------------------\n")
    
    # Close the session
    db.close()