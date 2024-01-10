import pandas as pd
from urllib.request import urlopen
import ics
import re
import arrow
from sqlalchemy import create_engine
import config
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import Boolean, Column, ForeignKey, BigInteger, Integer, String, TIMESTAMP, Table, MetaData
from IPython import display

engineURL = config.engineURL
engine = create_engine(engineURL, future=True)
metadata = MetaData()
conn = engine.connect()

users = Table(
    "users", metadata,
    Column("id", BigInteger, primary_key=True),
    Column("name", String)
)
    
events = Table(
    "events", metadata,
    Column("uid", String, primary_key=True),
    Column("name", String),
    Column("course", String),
    Column("description", String),
    Column("open", TIMESTAMP),
    Column("due", TIMESTAMP),
    Column("url", String),
    Column("modifiedDate", TIMESTAMP),
    Column("assignment", Boolean),
    Column("quiz", Boolean)
)

links = Table(
    "links", metadata,
    Column("index", Integer),
    Column("user_id", None, ForeignKey("users.id")),
    Column("event_id", None, ForeignKey("events.uid"))
)

metadata.create_all(engine)

def grabICS(url) -> pd.DataFrame:
    icsFile = ics.Calendar(urlopen(url).read().decode('utf-8'))
    icsDF = pd.DataFrame()
    now_year = arrow.now().date().year
    moodle_year = f"{str(now_year-1)[-2:]}{str(now_year)[-2:]}" if arrow.now().date().month < 9 else f"{str(now_year)[-2:]}{str(now_year+1)[-2:]}"
    for event in icsFile.events:
        eventDate = event.end.to('Asia/Hong_Kong')
        icsDF = pd.concat([icsDF, pd.DataFrame({'name':[event.name], \
                                                'course':[event.categories], \
                                                'description':[event.description], \
                                                'due':[eventDate.datetime], \
                                                'url':[f"https://moodle{moodle_year}.vtc.edu.hk/calendar/view.php?view=day&time={int(eventDate.timestamp())}"], \
                                                'modifiedDate':[event.last_modified.to('Asia/Hong_Kong').datetime]}, \
                                                index = [event.uid], \
                                                )])
    return icsDF

def grabDeadlines(df) -> pd.DataFrame:
    deadlinesDF = pd.concat([grabAssaignment(df), grabQuiz(df)]).sort_values('due', ascending=1)
    return deadlinesDF

def grabAssaignment(df) -> pd.DataFrame:
    assignmentDF = df[df['name'].str.contains('is due')]
    assignmentDF['name'] = assignmentDF['name'].apply(lambda x: re.sub(' is due', '', x))
    assignmentDF['course'] = assignmentDF['course'].apply(lambda x: list(x)[0])
    return assignmentDF

def grabQuiz(df) -> pd.DataFrame:
    quizDF = df[df['name'].str.contains('closes')]
    quizDF['name'] = quizDF['name'].apply(lambda x: re.sub(' closes', '', x))
    quizDF['course'] = quizDF['course'].apply(lambda x: list(x)[0])
    return quizDF

def grabQuizOpen(df) -> pd.DataFrame:
    quiz_startDF = df[df['name'].str.contains('opens')]
    quiz_startDF['name'] = quiz_startDF['name'].apply(lambda x: re.sub(' opens', '', x))
    quiz_startDF['course'] = quiz_startDF['course'].apply(lambda x: list(x)[0])
    quiz_startDF.columns = ['name', 'course', 'description', 'open', 'url', 'modifiedDate']
    return quiz_startDF

def sync(id, name, url):
    # conn.execute(users.insert(), {"id":id, "name":name})
    update_user_stmt = insert(users).values(id = id, name = name).on_conflict_do_update(index_elements=[users.c.id], set_ = dict(name = name))
    with engine.connect() as conn:
        result = conn.execute(update_user_stmt)
        conn.commit()
    icsDF = grabICS(url)
    for index, event in grabAssaignment(icsDF).iterrows():
        update_event_stmt = insert(events).values(uid = index, name = event[0], course = event[1], description = event[2], open = None, due = event[3], url = event[4], modifiedDate = event[5], assignment = True, quiz = False).on_conflict_do_update(index_elements=[events.c.uid], set_ = dict(name = event[0], course = event[1], description = event[2], open = None, due = event[3], url = event[4], modifiedDate = event[5], assignment = True, quiz = False))
        with engine.connect() as conn:
            result = conn.execute(update_event_stmt)
            conn.commit()
    for index, event in grabQuiz(icsDF).iterrows():
        update_event_stmt = insert(events).values(uid = index, name = event[0], course = event[1], description = event[2], open = None, due = event[3], url = event[4], modifiedDate = event[5], assignment = False, quiz = True).on_conflict_do_update(index_elements=[events.c.uid], set_ = dict(name = event[0], course = event[1], description = event[2], open = None, due = event[3], url = event[4], modifiedDate = event[5], assignment = False, quiz = True))
        with engine.connect() as conn:
            result = conn.execute(update_event_stmt)
            conn.commit()
    for index, event in grabQuizOpen(icsDF).iterrows():
        update_event_stmt = insert(events).values(uid = index, name = event[0], course = event[1], description = event[2], open = event[3], due = None, url = event[4], modifiedDate = event[5], assignment = False, quiz = True).on_conflict_do_update(index_elements=[events.c.uid], set_ = dict(name = event[0], course = event[1], description = event[2], open = None, due = event[3], url = event[4], modifiedDate = event[5], assignment = False, quiz = True))
        with engine.connect() as conn:
            result = conn.execute(update_event_stmt)
            conn.commit()
    for index in icsDF.index:
        pd.DataFrame({"user_id":[id], "event_id":[index]}).to_sql("links", con=engine, if_exists="append")
    pd.read_sql("SELECT user_id, event_id FROM links", engineURL).drop_duplicates().reset_index(drop=True).to_sql("links", con=engine, if_exists="replace")
    return(f"({id}) {name} has been synced")