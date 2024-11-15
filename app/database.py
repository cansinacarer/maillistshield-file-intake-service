from decouple import config
from datetime import datetime, timezone, timedelta
import pytz

DATABASE_CONNECTION_STRING = config("DATABASE_CONNECTION_STRING")

# Timezone used in this app
appTimezoneStr = config("TIMEZONE")
appTimezone = pytz.timezone(appTimezoneStr)

from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base, relationship


# Create an engine
engine = create_engine(DATABASE_CONNECTION_STRING)

# Define a base class for declarative class definitions
Base = declarative_base()


# Define the BatchJobs class mapped to the BatchJobs table
class BatchJobs(Base):
    __tablename__ = "BatchJobs"

    id = Column(Integer, primary_key=True)
    uid = Column(String(120), nullable=False, unique=True)
    # user_id = Column(Integer, ForeignKey("Users.id"), nullable=False)
    uploaded_file = Column(String(120), nullable=False)
    results_file = Column(String(120), nullable=True)
    completed_length = Column(Integer, nullable=False)
    file_length = Column(Integer, nullable=False)
    source = Column(String(120), nullable=False)
    status = Column(String(120), nullable=False)
    uploaded = Column(
        DateTime(),
        nullable=False,
        default=datetime.now(timezone.utc).astimezone(appTimezone),
    )
    started = Column(
        DateTime(),
        nullable=True,
    )
    finished = Column(DateTime(), nullable=True)
    result = Column(String(120), nullable=True)


# Create a session
Session = sessionmaker(bind=engine)
session = Session()

# Query the BatchJobs table
batch_jobs = session.query(BatchJobs).all()


def update_job_status(file, **kwargs):
    job = session.query(BatchJobs).filter_by(uploaded_file=file).first()
    for key, value in kwargs.items():
        setattr(job, key, value)
    session.commit()


def file_has_a_job_in_db(file):
    return session.query(BatchJobs).filter_by(uploaded_file=file).first() is not None
