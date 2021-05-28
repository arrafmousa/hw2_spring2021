from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Query import Query
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql

conn = None


def toRAM(data):
    return RAM(ramID=data['id'],
               company=data['company'],
               size=data['ramsize'])


def toQuery(data):
    return Query(queryID=data['id'],
                 purpose=data['purpose'],
                 size=data['size'])


def toDisk(data):
    return Disk(diskID=data['id'],
                company=data['manufacture'],
                speed=data['speed'],
                free_space=data['freesize'],
                cost=data['costbybyte']
                )


def dbConnection():
    global conn
    if conn is None:
        conn = Connector.DBConnector()
    return conn


def executeAndCommit(sql_query):
    rows, result = dbConnection().execute(sql_query)
    dbConnection().commit()
    return rows, result


def createQueryTable():
    try:
        query = sql.SQL("CREATE TABLE  Query ("
                        "id INTEGER PRIMARY KEY NOT NULL,"
                        "purpose TEXT NOT NULL,"
                        "size INTEGER NOT NULL"
                        ") ")
        executeAndCommit(query)
    except Exception as e:
        pass


def createDiskTable():
    try:
        query = sql.SQL("CREATE TABLE Disk ("
                        "id INTEGER PRIMARY KEY NOT NULL,"
                        "manufacture TEXT NOT NULL,"
                        "speed INT NOT NULL,"
                        "freeSize INT NOT NULL,"
                        "totalSize INT NOT NULL,"
                        "costByByte INT NOT NULL,"
                        "CHECK (freeSize >= 0),"
                        "CHECK (freeSize <= totalSize)"
                        ")")
        executeAndCommit(query)
    except Exception as e:
        pass


def createRAMTable():
    try:

        query = sql.SQL("CREATE TABLE RAM("
                        "id INTEGER PRIMARY KEY,"
                        "ramSize INTEGER NOT NULL,"
                        "company TEXT NOT NULL,"
                        "DiskId INT REFERENCES disk(id) ON DELETE CASCADE "
                        ")")
        executeAndCommit(query)
    except Exception as e:
        print(e)


def createRunsOnTable():
    try:

        query = sql.SQL("CREATE TABLE RunsON ("
                        "DiskId INTEGER,"
                        "QueryId INTEGER,"
                        "FOREIGN KEY (DiskId) REFERENCES Disk(id) ON DELETE CASCADE ,"
                        "FOREIGN KEY (QueryId) REFERENCES Query(id) ON DELETE CASCADE ,"
                        "UNIQUE (DiskId,QueryId)"
                        ")")
        executeAndCommit(query)
    except Exception as e:
        pass


def createAttachedToTable():
    try:

        query = sql.SQL(f"CREATE TABLE AttachedTo ("
                        f"RAMID INTEGER,"
                        f"DiskId INTEGER,"
                        f"FOREIGN KEY (RAMID) REFERENCES RAM(id) ON DELETE CASCADE ,"
                        f"FOREIGN KEY (DiskId) REFERENCES Disk(id) ON DELETE CASCADE,"
                        f"PRIMARY KEY (RAMID,DiskId));")
        executeAndCommit(query)
    except Exception as e:
        pass


def createTables():
    createQueryTable()
    createDiskTable()
    createRAMTable()
    createRunsOnTable()
    createAttachedToTable()


def clearTables():
    pass


def dropTable(table_name):
    query = sql.SQL(f"DROP TABLE {table_name} CASCADE")
    executeAndCommit(query)


def dropTables():
    dropTable("disk")
    dropTable("ram")
    dropTable('query')
    dropTable('runson')
    dropTable('attachedto')


def addQuery(query: Query) -> ReturnValue:
    if query is None or query == Query.badQuery():
        return ReturnValue.BAD_PARAMS
    result = ReturnValue.OK
    try:
        sql_query = sql.SQL(
            f"INSERT INTO query (id, purpose, size) VALUES ({query.getQueryID()},'{query.getPurpose()}',{query.getSize()})")
        executeAndCommit(sql_query)
    except DatabaseException.UNIQUE_VIOLATION:
        result = ReturnValue.ALREADY_EXISTS
    except Exception:
        result = ReturnValue.ERROR
    dbConnection().rollback()
    return result


def getQueryProfile(queryID: int) -> Query:
    try:
        sql_query = sql.SQL(f"SELECT * FROM query WHERE id = {queryID}")
        rows, result = executeAndCommit(sql_query)
        if rows == 0:
            return Query.badQuery()
        result = result[0]
        query = toQuery(result)
        return query
    except Exception as e:
        dbConnection().rollback()
    return Query.badQuery()


def deleteQuery(query: Query) -> ReturnValue:
    try:
        sql_query = sql.SQL(
            f"UPDATE disk SET freeSize = freeSize + (SELECT size FROM query WHERE id = {query.getQueryID()})"
            f"WHERE id IN ((SELECT diskid FROM runson WHERE queryid = {query.getQueryID()}));"
            f"DELETE FROM query WHERE id = {query.getQueryID()}")

        executeAndCommit(sql_query)
    except Exception as e:
        dbConnection().rollback()
        return ReturnValue.ERROR
    return ReturnValue.OK


def addDisk(disk: Disk) -> ReturnValue:
    if disk is None or disk == Disk.badDisk():
        return ReturnValue.BAD_PARAMS
    try:
        sql_query = sql.SQL(
            f"INSERT INTO disk (id, manufacture, speed, freesize,totalSize, costbybyte) VALUES "
            f"({disk.getDiskID()},"
            f"'{disk.getCompany()}',"
            f"{disk.getSpeed()},"
            f"{disk.getFreeSpace()},"
            f"{disk.getFreeSpace()},"
            f"{disk.getCost()}"
            f")")
        executeAndCommit(sql_query)
    except DatabaseException.UNIQUE_VIOLATION:
        dbConnection().rollback()
        return ReturnValue.ALREADY_EXISTS
    except Exception:
        dbConnection().rollback()
        return ReturnValue.ERROR

    return ReturnValue.OK


def getDiskProfile(diskID: int) -> Disk:
    try:
        sql_query = sql.SQL(f"SELECT * FROM disk WHERE id = {diskID}")
        rows, result = executeAndCommit(sql_query)
        if rows != 1:
            return Disk.badDisk()
        result = result[0]
        return toDisk(result)
    except Exception as e:
        dbConnection().rollback()
    return Disk.badDisk()


def deleteDisk(diskID: int) -> ReturnValue:
    try:
        sql_query = sql.SQL(f"DELETE FROM disk WHERE id = {diskID}")
        rows, result = executeAndCommit(sql_query)
        if rows == 0:
            return ReturnValue.NOT_EXISTS
    except Exception:
        dbConnection().rollback()
        return ReturnValue.ERROR
    return ReturnValue.OK


# INSERT INTO ram VALUE ()
def addRAM(ram: RAM) -> ReturnValue:
    if ram is None or ram == ram.badRAM():
        return ReturnValue.BAD_PARAMS
    try:
        sql_query = sql.SQL(
            f"INSERT INTO ram (id, ramsize, company)  VALUES "
            f"({ram.getRamID()},"
            f"{ram.getSize()},"
            f"'{ram.getCompany()}'"
            f")")
        executeAndCommit(sql_query)
    except DatabaseException.UNIQUE_VIOLATION:
        dbConnection().rollback()
        return ReturnValue.ALREADY_EXISTS
    except Exception:
        dbConnection().rollback()
        return ReturnValue.ERROR

    return ReturnValue.OK


def getRAMProfile(ramID: int) -> RAM:
    try:
        sql_query = sql.SQL(f"SELECT * FROM ram WHERE id = {ramID}")
        rows, result = executeAndCommit(sql_query)
        if rows != 1:
            return RAM.badRAM()
        result = result[0]
        return toRAM(result)
    except Exception as e:
        dbConnection().rollback()
    return RAM.badRAM()


def deleteRAM(ramID: int) -> ReturnValue:
    try:
        sql_query = sql.SQL(f"DELETE FROM disk WHERE id = {ramID}")
        rows, result = executeAndCommit(sql_query)
        if rows == 0:
            return ReturnValue.NOT_EXISTS
    except Exception:
        dbConnection().rollback()
        return ReturnValue.ERROR
    return ReturnValue.OK


def addDiskAndQuery(disk: Disk, query: Query) -> ReturnValue:
    try:
        sql_query = sql.SQL(f"INSERT INTO disk (id, manufacture, speed, freesize, costbybyte) VALUES "
                            f"({disk.getDiskID()},"
                            f"'{disk.getCompany()}',"
                            f"{disk.getSpeed()},"
                            f"{disk.getFreeSpace()},"
                            f"{disk.getCost()}"
                            f");"
                            f"INSERT INTO query (id, purpose, size) VALUES ("
                            f"{query.getQueryID()},"
                            f"'{query.getPurpose()}',"
                            f"{query.getSize()})")
        executeAndCommit(sql_query)
    except DatabaseException.UNIQUE_VIOLATION:
        dbConnection().rollback()
        return ReturnValue.ALREADY_EXISTS
    except Exception as e:
        # print(e)
        dbConnection().rollback()
        return ReturnValue.ERROR
    return ReturnValue.OK


def addQueryToDisk(query: Query, diskID: int) -> ReturnValue:
    try:
        sql_query = sql.SQL(
            f"UPDATE disk SET freeSize = freeSize - (SELECT size FROM query WHERE id = {query.getQueryID()})"
            f"WHERE id = {diskID};"
            f"INSERT INTO runson (diskid,queryid) VALUES ({diskID}, {query.getQueryID()})")
        executeAndCommit(sql_query)
    except DatabaseException.UNIQUE_VIOLATION:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return ReturnValue.NOT_EXISTS
    except Exception:
        return ReturnValue.ERROR
    return ReturnValue.OK


def removeQueryFromDisk(query: Query, diskID: int) -> ReturnValue:
    try:
        sql_query = sql.SQL(
            f"UPDATE disk set freeSize = freeSize + (Select Size FROM query Where id = {query.getQueryID()})"
            f"Where id = {diskID};"
            f"DELETE FROM runson WHERE diskid = {diskID} AND queryid = {query.getQueryID()};"
        )
        executeAndCommit(sql_query)
    except DatabaseException as e:
        return ReturnValue.ERROR
    return ReturnValue.OK


def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    try:
        sql_query = sql.SQL(f"INSERT INTO attachedto (ramid, diskid) VALUES ({ramID},{diskID});"
                            f"DELETE FROM attachedto WHERE ramid = {ramID} AND diskid <> {diskID};")
        rows, result = dbConnection().execute(sql_query)
    except DatabaseException.UNIQUE_VIOLATION:
        dbConnection().rollback()
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        dbConnection().rollback()
        return ReturnValue.NOT_EXISTS
    except Exception as e:
        dbConnection().rollback()
        return ReturnValue.ERROR
    dbConnection().commit()
    return ReturnValue.OK


def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    try:
        sql_query = sql.SQL(f"DELETE FROM attachedto WHERE ramid={ramID} AND diskid={diskID}")
        rows, result = dbConnection().execute(sql_query)
        if rows == 0:
            return ReturnValue.NOT_EXISTS
    except Exception as e:
        dbConnection().rollback()
        return ReturnValue.ERROR
    dbConnection().commit()
    return ReturnValue.OK


def averageSizeQueriesOnDisk(diskID: int) -> float:
    result = None
    try:
        sql_query = sql.SQL(f"SELECT AVG(size) "
                            f"FROM runson INNER JOIN query ON runson.queryid = query.id "
                            f"WHERE diskid = {diskID} ;")
        _, result = dbConnection().execute(sql_query)
        if result.rows[0][0] is None:
            return 0
    except Exception as e:
        print(e)
        return -1
    return result.rows[0][0]


def diskTotalRAM(diskID: int) -> int:
    result = None
    try:
        sql_query = sql.SQL(f"SELECT SUM(ramsize) "
                            f"FROM attachedto INNER JOIN ram ON ram.id = attachedto.ramid "
                            f"WHERE attachedto.diskid = {diskID} ")
        _, result = dbConnection().execute(sql_query)
    except Exception as e:
        print(e)
        return -1
    return result.rows[0][0]


def getCostForPurpose(purpose: str) -> int:
    try:
        sql_query = sql.SQL(f"SELECT SUM(size*costbybyte) "
                            f"FROM ("
                                f"SELECT disk.costbybyte , query.size"
                                f"FROM ("
                                    f"SELECT inner_size, disk.costbybyte AS outer_cost "
                                    f"FROM "
                                    f"("
                                    f"SELECT runson.diskid AS inner_diskid, query.size AS inner_size"
                                    f"query INNER JOIN runson ON query.id = runson.queryid "
                                    f"WHERE query.purpose = {purpose}"
                                    f")"
                                    f"INNER JOIN disk ON disk.id = inner_diskid"
                                f") "
                            f") "
                            )
    return 0


def getQueriesCanBeAddedToDisk(diskID: int) -> List[int]:
    return []


def getQueriesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    return []


def isCompanyExclusive(diskID: int) -> bool:
    return True


def getConflictingDisks() -> List[int]:
    return []


def mostAvailableDisks() -> List[int]:
    return []


def getCloseQueries(queryID: int) -> List[int]:
    return []


# dropTables()
# createTables()
# query = Query(queryID=1, purpose='stam', size=10)
# addQuery(query)
# deleteQuery(query)
# print(getQueryProfile(2))
# deleteQuery(query)
# disk1 = Disk(1, 'apple', 1, 2, 3)
# disk2 = Disk(2, 'samsung', 13, 50, 2)
# print(addQueryToDisk(query, disk1.getDiskID()))
# print(addQueryToDisk(query, disk2.getDiskID()))
# addDisk(disk)
# print(getDiskProfile(disk.getDiskID()))
# ram = RAM(10, 'aaaaaa', 100)
# addRAM(ram)
# print(addDiskAndQuery(disk, query))
# print(removeQueryFromDisk(query, 1))
# dropTable('attachedto')
# createAttachedToTable()
# print(addRAMToDisk(1,1))
# print(addRAMToDisk(1,2))
# print(addRAMToDisk(1,2))
# print(addRAMToDisk(999,3))
# print(addRAMToDisk(3,999))
# addRAMToDisk(1,1)
# addRAMToDisk(2,2)
# addRAMToDisk(3,3)
# addRAMToDisk(4,4)
#
# print(removeRAMFromDisk(999,1))
# print(removeRAMFromDisk(1,999))
# print(removeRAMFromDisk(999,999))
# print(removeRAMFromDisk(1,2))
# print(removeRAMFromDisk(999,1))
# print(removeRAMFromDisk(1,999))
# print(removeRAMFromDisk(1,1))

print(addRAMToDisk(1, 2))
print(addRAMToDisk(3, 2))

print(diskTotalRAM(2))
