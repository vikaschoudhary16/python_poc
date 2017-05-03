import os, os.path, datetime, shutil, MySQLdb


class FSUtil(object):
    source_dir = None
    dest_dir = None

    def __init__(this, source_dir, dest_dir):
        this.source_dir = source_dir
        this.dest_dir = dest_dir

    def move_files(this, filename):
        info = os.stat(os.path.join(this.source_dir, filename))
        dir_name = datetime.date.fromtimestamp(info.st_ctime)
        dest_address = os.path.join(this.dest_dir, str(dir_name))
        FSUtil.ensure_dir(dest_address)
        shutil.move(os.path.join(this.source_dir, str(filename)), dest_address)
        return os.path.join(dest_address, str(filename))

    @staticmethod
    def ensure_dir(dest_address):
        if not os.path.exists(dest_address):
            print dest_address
            os.makedirs(dest_address)


class DBUtil(object):
    cursor = None
    db = None

    def __init__(this, username, password, server, dbname):
        this.username = username
        this.password = password
        this.server = server
        this.dbname = dbname

    def prepare_cursor(this):
        this.db = MySQLdb.connect(this.server, this.username, this.password, this.dbname)
        this.cursor = this.db.cursor()

    def execute_query(this, query):
        this.cursor.execute(query)
        return this.cursor.fetchone()

    def commit_data(this):
        this.db.commit()
        this.db.close()

class FileMover(object):
    def __init__(this, fsutil, dbutil):
        this.fsutil = fsutil;
        this.dbutil = dbutil;

    def moveFiles(this):
        dbutil.prepare_cursor()
        for filename in os.listdir(fsutil.source_dir):
            filepath = fsutil.move_files(filename)
            query = "INSERT INTO Files(fileName, path) VALUES ('%s','%s'); " % (filename, filepath)
            print query
            dbutil.execute_query(query)
        dbutil.commit_data()


dbutil = DBUtil("root", "", "localhost", "TESTDB")
fsutil = FSUtil("/home/gauravs/Desktop/Input", "/home/garavs/Desktop/Output")

file_mover = FileMover(fsutil, dbutil)
file_mover.moveFiles()




