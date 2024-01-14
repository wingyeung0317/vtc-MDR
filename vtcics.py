import pandas as pd
from urllib.request import urlopen
import ics
import re
import arrow
from sqlalchemy import create_engine, delete
import config
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import Boolean, Column, ForeignKey, BigInteger, Integer, String, TIMESTAMP, Table, MetaData

engineURL = config.engineURL
engine = create_engine(engineURL, future=True)
metadata = MetaData()
conn = engine.raw_connection()
cursor = conn.cursor()

users = Table(
    "users", metadata,
    Column("id", BigInteger, primary_key=True),
    Column("name", String), 
    Column("url", String)
)

guilds = Table(
    "guilds", metadata,
    Column("id", BigInteger, primary_key=True),
    Column("name", String)
)

channels = Table(
    "channels", metadata,
    Column("id", BigInteger, primary_key=True),
    Column("name", String),
    Column("guild_id", None, ForeignKey("guilds.id")),
    Column("announce", Boolean, default=False)
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
    Column("type", String)
)

links = Table(
    "links", metadata,
    Column("index", Integer),
    Column("user_id", None, ForeignKey("users.id")),
    Column("event_id", None, ForeignKey("events.uid"))
)

user_in_guilds = Table(
    "user_in_guilds", metadata,
    Column("index", Integer),
    Column("user_id", None, ForeignKey("users.id")),
    Column("guild_id", None, ForeignKey("guilds.id"))
)
metadata.create_all(engine)

def grabICS(url) -> pd.DataFrame:
    icsDF = pd.DataFrame()
    icsFile = ics.Calendar(urlopen(url).read().decode('utf-8'))
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

def syncURL(id, name, url):
    old_df = pd.read_sql(f"SELECT links.event_id \
                                FROM events, links \
                                WHERE links.user_id = {id} AND events.uid = links.event_id ", 
                                engineURL).drop_duplicates().reset_index(drop=True)
    synced_df = pd.DataFrame()
    synced_viewer_df = pd.DataFrame()
    update_user_stmt = insert(users).values(id = id, name = name, url = url).on_conflict_do_update(index_elements=[users.c.id], set_ = dict(name = name, url=url))
    if not ((url == "") or (str(url) == "None")):
        with engine.connect() as conn:
            result = conn.execute(update_user_stmt)
            conn.commit()
            icsDF = grabICS(url)
        for index, event in grabAssaignment(icsDF).iterrows():
            update_event_stmt = insert(events).values(uid = index, name = event[0], course = event[1], description = event[2], open = None, due = event[3], url = event[4], modifiedDate = event[5], type = "assignment").on_conflict_do_update(index_elements=[events.c.uid], set_ = dict(name = event[0], course = event[1], description = event[2], open = None, due = event[3], url = event[4], modifiedDate = event[5], type = "assignment"))
            with engine.connect() as conn:
                result = conn.execute(update_event_stmt)
                conn.commit()
        for index, event in grabQuiz(icsDF).iterrows():
            update_event_stmt = insert(events).values(uid = index, name = event[0], course = event[1], description = event[2], open = None, due = event[3], url = event[4], modifiedDate = event[5], type = "quiz").on_conflict_do_update(index_elements=[events.c.uid], set_ = dict(name = event[0], course = event[1], description = event[2], open = None, due = event[3], url = event[4], modifiedDate = event[5], type = "quiz"))
            with engine.connect() as conn:
                result = conn.execute(update_event_stmt)
                conn.commit()
        for index, event in grabQuizOpen(icsDF).iterrows():
            update_event_stmt = insert(events).values(uid = index, name = event[0], course = event[1], description = event[2], open = event[3], due = None, url = event[4], modifiedDate = event[5], type = "quiz").on_conflict_do_update(index_elements=[events.c.uid], set_ = dict(name = event[0], course = event[1], description = event[2], open = event[3], due = None, url = event[4], modifiedDate = event[5], type = "quiz"))
            with engine.connect() as conn:
                result = conn.execute(update_event_stmt)
                conn.commit()
        for index in icsDF.index:
            pd.DataFrame({"user_id":[id], "event_id":[index]}).to_sql("links", con=engine, if_exists="append")
        synced_df = pd.read_sql("SELECT user_id, event_id FROM links", engineURL).drop_duplicates().reset_index(drop=True)
        stmt = (delete(links))
        with engine.connect() as conn:
            result = conn.execute(stmt)
            conn.commit()
        synced_df.to_sql("links", con=engine, if_exists="append")
        synced_viewer_df = pd.read_sql(f"SELECT links.event_id, name, course, open, due \
                                    FROM events, links \
                                    WHERE links.user_id = {id} AND events.uid = links.event_id ", 
                                    engineURL, index_col='event_id').drop_duplicates()
        print(synced_viewer_df)
        synced_viewer_df = synced_viewer_df.drop(index=old_df['event_id']).reset_index(drop=True)
    msg = f"{len(synced_viewer_df)} events added (Total {len(synced_df)}): ```{synced_viewer_df}```"
    return(msg)

def sync_guild(guildDF:pd.DataFrame, userDF:pd.DataFrame):
    # update guild
    for index, guild in guildDF.drop_duplicates('guild_id').iterrows():
        stmt = insert(guilds).values(id = guild['guild_id'], name = guild['guild_name']).on_conflict_do_update(index_elements=[guilds.c.id], set_ = dict(name = guild['guild_name']))
        with engine.connect() as conn:
            result = conn.execute(stmt)
            conn.commit()
    # update channel
    for index, channel in guildDF.iterrows():
        stmt = insert(channels).values(id = channel['id'], name = channel['name'], guild_id = channel['guild_id']).on_conflict_do_update(index_elements=[channels.c.id], set_ = dict(name = channel['name'], guild_id = channel['guild_id']))
        with engine.connect() as conn:
            result = conn.execute(stmt)
            conn.commit()
    # update user and guild belonings
    for index, user in userDF.iterrows():
        update_user_stmt = insert(users).values(id = user['user_id'], name = user['name']).on_conflict_do_update(index_elements=[users.c.id], set_ = dict(name = user['name']))
        with engine.connect() as conn:
            result = conn.execute(update_user_stmt)
            conn.commit()
        cursor.execute(f"SELECT url FROM users WHERE id = {user['user_id']}")
        url = cursor.fetchone()
        syncURL(user['user_id'], user['name'], url[0])
    userDF[['user_id', 'guild_id']].reset_index(drop=True).to_sql("user_in_guilds", con=engine, if_exists="append")
    user_in_guilds_df = pd.read_sql("SELECT user_id, guild_id FROM user_in_guilds", engineURL).drop_duplicates().reset_index(drop=True)
    stmt = (delete(user_in_guilds))
    with engine.connect() as conn:
        result = conn.execute(stmt)
        conn.commit()
    user_in_guilds_df.to_sql("user_in_guilds", con=engine, if_exists="append")

def grab_announce_channel():
    announce_channelDF = pd.read_sql("SELECT id FROM channels WHERE announce = TRUE", engineURL)
    return announce_channelDF

def grab_announce_user():
    announce_userDF = pd.read_sql("SELECT id FROM users", engineURL).reset_index(drop=True)
    announce_userDF = announce_userDF.drop(announce_userDF[announce_userDF['id']==1194240403008933999].index)
    return announce_userDF

def mute_channel(id) -> Boolean:
    cursor.execute(f"SELECT announce FROM channels WHERE id = {id}")
    mute_bool = cursor.fetchone()
    mute_bool = not mute_bool[0]
    stmt = channels.update().values(announce = mute_bool).where(channels.c.id == id)
    with engine.connect() as conn:
        result = conn.execute(stmt)
        conn.commit()
    cursor.execute(f"SELECT announce FROM channels WHERE id = {id}")
    mute_bool = cursor.fetchone()
    return mute_bool[0]

def checkTime(seconds):
    time_now = arrow.now()
    # time_now = arrow.Arrow(2024, 4, 19, 23, 40, 0)
    time_shift = seconds + (seconds/2/2)
    open_events = pd.read_sql(f" \
                                  SELECT l.user_id, e.name, e.description, e.course, e.open, e.type, e.url \
                                  FROM links AS l \
                                  INNER JOIN events AS e ON l.event_id = e.uid \
                                  WHERE open BETWEEN '{time_now}' AND '{time_now.shift(seconds=time_shift).datetime}'", engineURL)
    opened_events = pd.read_sql(f" \
                                  SELECT l.user_id, e.name, e.description, e.course, e.open, e.type, e.url \
                                  FROM links AS l \
                                  INNER JOIN events AS e ON l.event_id = e.uid \
                                  WHERE open BETWEEN '{time_now.shift(seconds=time_shift*-1).datetime}' AND '{time_now}'", engineURL)
    due_events = pd.read_sql(f" \
                                  SELECT l.user_id, e.name, e.description, e.course, e.due, e.type, e.url \
                                  FROM links AS l \
                                  INNER JOIN events AS e ON l.event_id = e.uid \
                                  WHERE due BETWEEN '{time_now.shift(seconds=time_shift*-1).datetime}' AND '{time_now.shift(seconds=time_shift).datetime}'", engineURL)
    return open_events, opened_events, due_events