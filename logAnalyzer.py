#!/usr/bin/python
# Database code for the DB news and Log Analyzer by Ricky Tham.
import sys
import getopt
import os
import psycopg2

DBNAME = "news"
hHelpStr = "usage: logAnalyzer.py [-h] [-o PATH] [-q [0-3]]\n\n" + \
    "optional arguments:\n" + \
           "   -h, --help                 show this help message and exit\n"
hOutputStr = "   -o PATH, --output PATH     set output path\n"
hQueryStr = "   -q [0-3], --query [0-3]    set query value to execute\n" + \
    "                              specific queries.\n" + \
    "                                 0: Runs all queries\n" + \
    "                                 1: Runs the first query\n" + \
    "                                 2: Runs the second query\n" + \
    "                                 3: Runs the third query\n"


def executeQuery(query):
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute(query)
    results = c.fetchall()
    db.close()
    return results


def getFirstSQLQuery(rFile):
    """
    Gets a
    """
    results = executeQuery(
        "select articles.title, count(articles.title) as num from articles," +
        "    (select substring(path from 10) as tpath from log" +
        "        where path != '/') as logpath" +
        "    where logpath.tpath = articles.slug" +
        "    group by articles.title" +
        "    order by num desc" +
        "    limit 3;")
    lines = "1. What are the most popular three articles of all time?\n"
    lines += "".join('"{}" -- {} views\n'.format(title, views)
                     for title, views in results)
    lines += "\n"
    print(lines)
    rFile.write(lines)


def getSecondSQLQuery(rFile):
    results = executeQuery(
        "select authors.name, authorViewCount.counter from authors, " +
        "    (select articles.author, count(articles.author) as counter " +
        "    from articles, logpath where logpath.tpath = articles.slug " +
        "    group by articles.author order by counter desc) " +
        "    as authorViewCount where authors.id = authorViewCount.author;")
    lines = "2. Who are the most popular article authors of all time?\n"
    lines += "".join('{} -- {} views\n'.format(title, views)
                     for title, views in results)
    lines += "\n"
    print(lines)
    rFile.write(lines)


def getThirdSQLQuery(rFile):
    results = executeQuery(
        "select dailyErrors.d," +
        "(round((cast(dailyErrors.dcount as numeric) / dailyRequest.dcount)" +
        " * 100, 2)) as num from" +
        "(select date(time) as d, count(*) as dcount from log " +
        "where substring(status from 1 for 1) != '2' group by d) as " +
        "dailyErrors join (select date(time) as d, count(*) as " +
        "dcount from log group by d) as dailyRequest " +
        "on dailyErrors.d = dailyRequest.d " +
        "where ((cast(dailyErrors.dcount as decimal) / dailyRequest.dcount) "
        "* 100) > 1;")
    lines = "3. On which days did more than 1% of requests lead to errors?\n"
    lines += "".join('{} -- {}% errors\n'.format(date, errors)
                     for date, errors in results)
    lines += "\n"
    print(lines)
    rFile.write(lines)


def createResultPath(path):
    if not os.path.exists(os.path.dirname(path)):
        try:
            os.makedirs(os.path.dirname(path))
        except OSError as exc:
            print('Failed to create the directory "{}" with error: {}'.format(
                path, exc))


def main(argv):
    shortOptions = "ho:q:"
    longOptions = ["help", "output=", "query="]
    path = "output/logResults.txt"
    runQuery = 0
    try:
        arguments, values = getopt.getopt(argv, shortOptions, longOptions)
    except getopt.error as err:
        print(str(err))
        sys.exit(2)
    for currArg, currValue in arguments:
        if currArg in ("-h", "--help"):
            print(hHelpStr + hOutputStr + hQueryStr)
            sys.exit()
        elif currArg in ("-o", "--output"):
            print("Result path changed to: " + currValue)
            path = str(currValue)
        elif currArg in ("-q", "--query"):
            runQuery = int(currValue)
            if (runQuery < 0 or runQuery > 3):
                print("Query must be between 0 to 3 inclusive\n" + hQueryStr)
                sys.exit()
    createResultPath(path)
    resultsFile = open(path, 'w+')
    if (runQuery == 0 or runQuery == 1):
        getFirstSQLQuery(resultsFile)
    if (runQuery == 0 or runQuery == 2):
        getSecondSQLQuery(resultsFile)
    if (runQuery == 0 or runQuery == 3):
        getThirdSQLQuery(resultsFile)
    resultsFile.close()


if __name__ == '__main__':
    main(sys.argv[1:])
