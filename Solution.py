from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Query import Query
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql


def createQueryTable():
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("CREATE TABLE  Query ("
                        "id INTEGER PRIMARY KEY NOT NULL,"
                        "purpose TEXT NOT NULL,"
                        "size INTEGER NOT NULL"
                        ") ")
        conn.execute(query)
        conn.commit()
        conn.close()
    except Exception as e:
        pass


def createDiskTable():
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("CREATE TABLE Disk ("
                        "id INTEGER PRIMARY KEY NOT NULL,"
                        "manufacture TEXT NOT NULL,"
                        "speed INT NOT NULL,"
                        "freeSize INT NOT NULL,"
                        "costByByte INT NOT NULL"
                        ")")
        conn.execute(query)
        conn.commit()
        conn.close()
    except Exception as e:
        pass


def createRAMTable():
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("CREATE TABLE RAM("
                        "id INTEGER PRIMARY KEY,"
                        "ramSize INTEGER NOT NULL,"
                        "company TEXT NOT NULL,"
                        "DiskId INT REFERENCES disk(id)"
                        ")")
        conn.execute(query)
        conn.commit()
        conn.close()
    except Exception as e:
        print(e)


def createRunsOnTable():
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("CREATE TABLE RunsON ("
                        "DiskId INTEGER,"
                        "QueryId INTEGER,"
                        "FOREIGN KEY (DiskId) REFERENCES Disk(id),"
                        "FOREIGN KEY (QueryId) REFERENCES Query(id),"
                        "UNIQUE (DiskId,QueryId)"
                        ")")
        conn.execute(query)
        conn.commit()
        conn.close()
    except Exception as e:
        pass


def createTables():
    createQueryTable()
    createDiskTable()
    createRAMTable()
    createRunsOnTable()


def clearTables():
    pass


def dropTable(table_name):
    conn = Connector.DBConnector()
    query = sql.SQL(f"DROP TABLE {table_name}")
    conn.execute(query)
    conn.commit()
    conn.close()


def dropTables():
    dropTable("ram")


def addQuery(query: Query) -> ReturnValue:
    return ReturnValue.OK


def getQueryProfile(queryID: int) -> Query:
    return Query()


def deleteQuery(query: Query) -> ReturnValue:
    return ReturnValue.OK


def addDisk(disk: Disk) -> ReturnValue:
    return ReturnValue.OK


def getDiskProfile(diskID: int) -> Disk:
    return Disk()


def deleteDisk(diskID: int) -> ReturnValue:
    return ReturnValue.OK


def addRAM(ram: RAM) -> ReturnValue:
    return ReturnValue.OK


def getRAMProfile(ramID: int) -> RAM:
    return RAM()


def deleteRAM(ramID: int) -> ReturnValue:
    return ReturnValue.OK


def addDiskAndQuery(disk: Disk, query: Query) -> ReturnValue:
    return ReturnValue.OK


def addQueryToDisk(query: Query, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def removeQueryFromDisk(query: Query, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def averageSizeQueriesOnDisk(diskID: int) -> float:
    return 0


def diskTotalRAM(diskID: int) -> int:
    return 0


def getCostForPurpose(purpose: str) -> int:
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


dropTables()
createTables()
