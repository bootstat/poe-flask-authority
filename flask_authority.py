import os
import time,random
import sqlite3 
from sqlite3 import Error
from trade_api.poe_trade_interface import POETradeInterface

def create_database(db_path):
    try:
        conn = sqlite3.connect(db_path)
        # Generate tables, i can clean this up so it's not four big blocks in the future
        flask_url_sc_table = """ CREATE TABLE IF NOT EXISTS flask_urls_sc (
                                id integer PRIMARY KEY,
                                flask_name text NOT NULL,
                                flask_url text NOT NULL
                            ); """
        flask_price_sc_table = """ CREATE TABLE IF NOT EXISTS flask_prices_sc (
                                    id integer PRIMARY KEY,
                                    flask_name text NOT NULL,
                                    flask_base text NOT NULL,
                                    flask_price integer NOT NULL,
                                    flask_price_currency text NOT NULL
                            ); """
        flask_url_hc_table = """ CREATE TABLE IF NOT EXISTS flask_urls_hc (
                                id integer PRIMARY KEY,
                                flask_name text NOT NULL,
                                flask_url text NOT NULL                            
                            ); """
        flask_price_hc_table = """ CREATE TABLE IF NOT EXISTS flask_prices_hc (
                                    id integer PRIMARY KEY,
                                    flask_name text NOT NULL,
                                    flask_base text NOT NULL,
                                    flask_price integer NOT NULL,
                                    flask_price_currency text NOT NULL                            
                            ); """
        conn.execute
        cursor = conn.cursor()
        cursor.execute(flask_url_sc_table)
        cursor.execute(flask_url_hc_table)
        cursor.execute(flask_price_sc_table)
        cursor.execute(flask_price_hc_table)
    except Error as e:
        print(e)
    finally:
        conn.commit()
        conn.close()

def connect_database(db_path):
    try:
        conn = sqlite3.connect(db_path)
        return conn
    except Error as e:
        print(e)  
        return None

def query_database(db_path, query):
    # need to add with closing to this in the future
    conn = connect_database(db_path) 
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    return cursor

def modify_flask_url_table(db_path, run, league_type):
    conn = connect_database(db_path)
    if league_type=="sc":
        update_flask = """ INSERT INTO flask_urls_sc(flask_name, flask_url) VALUES(?,?) """
    elif league_type=="hc":
        update_flask = """ INSERT INTO flask_urls_hc(flask_name, flask_url) VALUES(?,?) """
    cursor = conn.cursor()
    cursor.execute(update_flask, run)
    conn.commit()
    conn.close()

def populate_flask_tables(flask_array, resume):
    # resume is either an int of value 0, or a list formatted as: [1, "Flask Name"]
    # print("resume var type and value " + str(type(resume)))
    if type(resume) is int: 
        if resume!=0:
            # set resume to 0
            resume = 0
    elif type(resume) is list: 
        if resume[0]!=1:
            # improperly formatted, set resume to 0
            resume = 0
        else:
            # basic checking for flask name
            if "flask".lower() in resume[1].lower():
                flask_name = resume[1]
                resume = 1
    
    if resume==1:
        # process resume from specific index+1
        print("resuming flask url import run")
        index = flask_array.index(flask_name)
        for flask in flask_array[index+1:]:
            sc_args = {"name":flask, "league":["Legion"]}
            hc_args = {"name":flask, "league":["Hardcore Legion"]}
            sc_url = interface.get_query_url(sc_args)
            hc_url = interface.get_query_url(hc_args)
            
            # add flask results to default tables
            sc_run = (flask, sc_url)
            hc_run = (flask, hc_url)
            print("importing " + str(sc_run) + " | " + str(hc_url)) 
            modify_flask_url_table(flask_db_path, sc_run, "sc")
            modify_flask_url_table(flask_db_path, hc_run, "hc")
            
            # pause to be friendly to the server
            time.sleep(random.uniform(1,3))
    elif resume==0:
        # do not resume
        for flask in flask_array:
            sc_args = {"name":flask, "league":["Legion"]}
            hc_args = {"name":flask, "league":["Hardcore Legion"]}
            sc_url = interface.get_query_url(sc_args)
            hc_url = interface.get_query_url(hc_args)
            
            # add flask results default table    
            sc_run = (flask, sc_url)
            hc_run = (flask, hc_url)
            print("importing " + str(sc_run) + " | " + str(hc_url)) 
            modify_flask_url_table(flask_db_path, sc_run, "sc")
            modify_flask_url_table(flask_db_path, hc_run, "hc")
            
            # pause to be friendly to the server
            time.sleep(random.uniform(1,3))

# init interface
interface = POETradeInterface()

# flask arrays
flask_static = ["Flask", "of"]
flask_names = ["Divine Life", "Eternal Life", "Quicksilver", "Bismuth", "Stibnite", "Amethyst", "Ruby", "Sapphire", "Topaz", "Silver", "Aquamarine", "Granite", "Jade", "Quartz", "Sulphur", "Basalt", "Diamond"]
prefix = ["Alchemist's", "Ample", "Avenger's", "Bubbling", "Catalysed", "Caustic", "Cautious", "Chemist's", "Experimenter's", "Panicked", "Perpetual", "Sapping", "Saturated", "Seething", "Surgeon's"]
suffix = ["Adrenaline", "Animation", "Craving", "Curing", "Dousing", "Fending", "Gluttony", "Grounding", "Heat", "Iron Skin", "Reflexes", "Resistance", "Staunching", "Steadiness", "Warding"]

# Create flask name list
flask_array = []
for flask_name in flask_names:
    # Check every prefix 
    for prefix_name in prefix:
        # Check every suffix
        for suffix_name in suffix: 
            flask_array.append((prefix_name + " " + flask_name + " " + flask_static[0] + " " + flask_static[1] + " " + suffix_name))

# print(flask_array)
print("processing this many flasks: " + str(len(flask_array)))

# flask results
sc_results = []
hc_results = []

# create flask_authority database if not exists
flask_db = "flask_authority.db" 
flask_db_dir = os.path.join(os.path.expanduser("~"), "poe-flask-app/db")
flask_db_path = os.path.join(flask_db_dir, flask_db)
if not os.path.exists(flask_db_dir):
    os.makedirs(flask_db_dir)

if not os.access(flask_db_path, os.R_OK):
    # create database
    flask_db = "flask_authority.db"
    create_database(flask_db_path)

# database should contain flask_array*2 rows, check before populating
flask_url_tables = ["flask_urls_sc", "flask_urls_hc"]
row_count = 0
for table in flask_url_tables:
    query = "SELECT COUNT(*) from {}".format(table)
    cursor = query_database(flask_db_path, query)
    result = cursor.fetchone()
    row_count += result[0]

if row_count!=len(flask_array)*2:
    if row_count>0: 
        print("current db row count: " + str(row_count))
        # let's assume that there is some properly formatted data and try to resume from a position
        name_results = []
        for table in flask_url_tables:
            query = "SELECT * from {} WHERE id= (select max(id) from {});".format(table, table)
            cursor = query_database(flask_db_path, query)
            name_results.append(cursor.fetchone()[1])
        
        #print(name_results[0] + " :: " + name_results[1])
        if name_results[0]!=name_results[1]:
            print("comparing name results: " + name_results[0] + " :: " + name_results[1])
            # sc doesn't match hc, let's find the first row that has name equality and drop everything after that
            query = """select max(flask_urls_sc.id) FROM 
                        flask_urls_sc LEFT JOIN flask_urls_hc ON 
                        flask_urls_sc.flask_name = flask_urls_hc.flask_name WHERE 
                        flask_urls_sc.flask_name = flask_urls_hc.flask_name;"""
            cursor = query_database(flask_db_path, query)
            last_good_id = cursor.execute(query).fetchone()[0]
            
            # delete rows after our last good id
            for table in flask_url_tables:
                query = "DELETE FROM {} where id>{};".format(table, last_good_id)
                cursor = query_database(flask_db_path, query) 
           
            # restart flask url import execution, hardcoding sc since we should have name equality now
            query = "SELECT * from flask_urls_sc WHERE id= (select max(id) from flask_urls_sc);"
            cursor = query_database(flask_db_path, query)
            flask_name = cursor.fetchone()[1]
            resume = [1, flask_name]
            print("resuming from flask name: " + resume[1])
            populate_flask_tables(flask_array, resume)
    else: 
        print("populating database with new data")
        populate_flask_tables(flask_array, 0)


#sc_results = interface.get_cheapest.query_results(url) 
