import pandas as pd
from urllib.request import urlopen
import ics
import re
import arrow

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
                                                'due':[eventDate], \
                                                'url':[f"https://moodle{moodle_year}.vtc.edu.hk/calendar/view.php?view=day&time={int(eventDate.timestamp())}"], \
                                                'modifiedDate':[event.last_modified.to('Asia/Hong_Kong')]}, \
                                                index = [event.uid], \
                                                )])
    return icsDF

def grabDeadlines(url) -> pd.DataFrame:
    deadlinesDF = pd.concat([grabAssaignment(url), grabQuiz(url)]).sort_values('due', ascending=1)
    return deadlinesDF

def grabAssaignment(url) -> pd.DataFrame:
    icsDF = grabICS(url)
    assignmentDF = icsDF[icsDF['name'].str.contains('is due')]
    assignmentDF['name'] = assignmentDF['name'].apply(lambda x: re.sub(' is due', '', x))
    assignmentDF['course'] = assignmentDF['course'].apply(lambda x: list(x)[0])
    return assignmentDF

def grabQuiz(url) -> pd.DataFrame:
    icsDF = grabICS(url)
    quizDF = icsDF[icsDF['name'].str.contains('closes')]
    quizDF['name'] = quizDF['name'].apply(lambda x: re.sub(' closes', '', x))
    quizDF['course'] = quizDF['course'].apply(lambda x: list(x)[0])
    return quizDF

def grabQuizOpen(url) -> pd.DataFrame:
    icsDF = grabICS(url)
    quiz_startDF = icsDF[icsDF['name'].str.contains('opens')]
    quiz_startDF['name'] = quiz_startDF['name'].apply(lambda x: re.sub(' opens', '', x))
    quiz_startDF['course'] = quiz_startDF['course'].apply(lambda x: list(x)[0])
    quiz_startDF.columns = ['name', 'course', 'description', 'open', 'url', 'modifiedDate']
    return quiz_startDF