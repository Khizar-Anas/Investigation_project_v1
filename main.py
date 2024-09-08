import psycopg2
import psycopg2.extras
from config import config
import pandas as pd

def copy_from_csv(conn, cursor, table_name, csv_file_path):
    #Open the csv file
    with open('C:/Users/syedk/Documents/Investigation_project/homicide_article.csv', 'r', encoding='ISO-8859-1') as file:
        #copy data from the csv file to the table
        cursor.execute('SET datestyle = "ISO, DMY";')
        cursor.copy_expert("""COPY homicide_media.homicide_article (news_report_url, news_report_headline, news_report_platform, date_of_publication, author, wire_service, notes, no_of_subs)
        FROM STDIN WITH CSV HEADER DELIMITER ';'
        """, file)
        print(f"Data copied successfully to homicide_article.")
        
def copy_from_victim_csv(conn, cursor, table_name, csv_file_path):
    #Open the csv file
    with open('C:/Users/syedk/Documents/Investigation_project/victim_data.csv', 'r', encoding='ISO-8859-1') as file:
        #copy data from the csv file to the table
        cursor.execute('SET datestyle = "ISO, DMY";')
        cursor.copy_expert("""COPY homicide_victim.victim(news_report_url,
                            victim_name,  
                            date_of_death,
                            place_of_death_province,
                            place_of_death_town,
                            type_of_location,
                            sexual_assault,
                            race_of_victim,
                            age_of_victim,
                            mode_of_death_specific,
                            robbery_y_n_u)
        FROM STDIN WITH CSV HEADER DELIMITER ';'
        """, file)
        print(f"Data copied successfully to victim_data.")

# Function to automatically update the article_id in the victim table based on news_report_url
def update_victim_article_ids(cursor):
    # This query updates article_id in the victim table by matching news_report_url
    update_script = '''
        UPDATE homicide_victim.victim
        SET article_id = homicide_article.article_id
        FROM homicide_media.homicide_article
        WHERE homicide_victim.victim.news_report_url = homicide_article.news_report_url;
    '''
    cursor.execute(update_script)
    print("Article IDs updated in the victim table.")
    
def copy_from_perpetrator_csv(conn, cursor, table_name, csv_file_path):
    #Open the csv file
    with open('C:/Users/syedk/Documents/Investigation_project/perpetrator_data.csv', 'r', encoding='ISO-8859-1') as file:
        #copy data from the csv file to the table
        cursor.execute('SET datestyle = "ISO, DMY";')
        cursor.copy_expert("""COPY homicide_perpetrator.perpetrator(news_report_url,
                           perpetrator_name,
                           perpetrator_relationship_to_victim,
                           suspect_arrested,
                           suspect_convicted,
                           serial_killer,
                           intimate_femicide_y_n_u,
                           extreme_violence_y_n_m_u)
        FROM STDIN WITH CSV HEADER DELIMITER ';'
        """, file)
        print(f"Data copied successfully to perpetrator_data.")
        
def update_perpetrator_article_ids(cursor):
    # This query updates article_id in the perpetrator table by matching news_report_url
    update_script_perp = '''
        UPDATE homicide_perpetrator.perpetrator
        SET article_id = homicide_article.article_id
        FROM homicide_media.homicide_article
        WHERE homicide_perpetrator.perpetrator.news_report_url = homicide_media.homicide_article.news_report_url;
    '''
    cursor.execute(update_script_perp)
    print("Article IDs updated in the perpetrator table.")
    
def update_perpetrator_victim_id(cursor):
    # This query updates article_id in the perpetrator table by matching news_report_url
    update_script_perp = '''
        UPDATE homicide_perpetrator.perpetrator
        SET victim_id = victim.victim_id
        FROM homicide_victim.victim
        WHERE homicide_perpetrator.perpetrator.news_report_url = homicide_victim.victim.news_report_url;
    '''
    cursor.execute(update_script_perp)
    print("Article IDs updated in the perpetrator table.")

        

def connect():
    connection = None
    csr = None
    try:
        params = config()
        print('Connecting to the PostgreSQL database ...')
        connection = psycopg2.connect(**params)
        csr = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        create_schema_script1 = '''CREATE SCHEMA IF NOT EXISTS homicide_media;'''
        csr.execute(create_schema_script1)
        print("Schema 'homicide_media created or already exists")
        
        create_schema_script2 = '''CREATE SCHEMA IF NOT EXISTS homicide_victim;'''
        csr.execute(create_schema_script2)
        print("Schema 'homicide_victim is created or already exists")
        
        create_schema_script3 = '''CREATE SCHEMA IF NOT EXISTS homicide_perpetrator;'''
        csr.execute(create_schema_script3)
        print("Schema 'homicide_perpetrator is created or already exists")
        
        
        csr.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')
        # Drop the table if it already exists
        
        csr.execute("DROP TABLE IF EXISTS homicide_media.homicide_article CASCADE")
        csr.execute('DROP TABLE IF EXISTS homicide_victim.victim CASCADE')
        csr.execute('DROP TABLE IF EXISTS homicide_perpetrator.perpetrator CASCADE')
        
        create_script_article = '''CREATE TABLE homicide_media.homicide_article (
                            article_id SERIAL PRIMARY KEY,
                            news_report_id UUID DEFAULT uuid_generate_v4(), 
                            news_report_url VARCHAR(255) UNIQUE,
                            news_report_headline VARCHAR(255),
                            news_report_platform VARCHAR(255),
                            date_of_publication DATE,
                            author VARCHAR(255),
                            wire_service VARCHAR(255),
                            notes VARCHAR(1000),
                            no_of_subs INT
                            )'''                   
        csr.execute(create_script_article)
        print("homicide_article Table created successfully in homicide_media")
        copy_from_csv(connection, csr, 'homicide_media.homicide_article','C:/Users/syedk/Documents/Investigation_project/homicide_article.csv' )
        
        
        create_script_victim = '''CREATE TABLE homicide_victim.victim (
                            victim_id SERIAL PRIMARY KEY,
                            article_id INT REFERENCES homicide_media.homicide_article(article_id) ON DELETE CASCADE,
                            news_report_url VARCHAR(255),
                            victim_name VARCHAR(255),
                            date_of_death DATE,
                            place_of_death_province VARCHAR(100),
                            place_of_death_town VARCHAR(255),
                            type_of_location VARCHAR(255),
                            sexual_assault VARCHAR(255),
                            race_of_victim VARCHAR(255),
                            age_of_victim INT,
                            mode_of_death_specific VARCHAR(100),
                            robbery_y_n_u VARCHAR(10)
                            )'''                      
        csr.execute(create_script_victim)
        print("Victim Table is created successfully in homicide_victim")
        copy_from_victim_csv(connection, csr, 'homicide_victim.victim','C:/Users/syedk/Documents/Investigation_project/victim_data.csv' )
        # Automatically update article_id in the victim table by matching news_report_url
        update_victim_article_ids(csr)
        

        create_script_perpetrator = '''CREATE TABLE homicide_perpetrator.perpetrator (
                            perpetrator_id SERIAL PRIMARY KEY,
                            article_id INT REFERENCES homicide_media.homicide_article(article_id) ON DELETE CASCADE,
                            news_report_url VARCHAR(255),
                            perpetrator_name VARCHAR(255),
                            perpetrator_relationship_to_victim VARCHAR(255),
                            suspect_arrested VARCHAR(255),
                            suspect_convicted VARCHAR(255),
                            serial_killer BOOLEAN,
                            intimate_femicide_y_n_u VARCHAR(10),
                            extreme_violence_y_n_m_u VARCHAR(10),
                            victim_id INT REFERENCES homicide_victim.victim(victim_id) ON DELETE CASCADE
                            )'''                      
        csr.execute(create_script_perpetrator)
        print("Perpetrator Table created successfully in homicide_perpetrator")
        copy_from_perpetrator_csv(connection, csr, 'homicide_perpetrator.perpetrator','C:/Users/syedk/Documents/Investigation_project/perpetrator_data.csv' )
        update_perpetrator_article_ids(csr)
        update_perpetrator_victim_id(csr)
        
        
        
        
        
        
        # insert_script = 'INSERT INTO homicide_article (news_report_id, news_report_url, news_report_platform, author, news_report_headline) VALUES (%s, %s, %s, %s, %s)'
        # insert_values = [(0, 'soemsomesome', 'news24', 'Salman', 'people died due to killings'), (1, 'soemsome', 'new4', 'Salman', 'people died due to killings'), (3, 'soemsomme', 'ews24', 'Salman', 'people died due to killings')]
        # for record in insert_values: 
        #     csr.execute(insert_script, record)
            
        # update_script = 'UPDATE homicide_article SET author = %s' 
        # update_record = ('Abdul',) 
        # csr.execute(update_script, update_record)
        
        # delete_script = 'DELETE FROM homicide_article WHERE news_report_url = %s'
        # delete_record = ('soemsomesome',)
        # csr.execute(delete_script, delete_record)
        
       
        # csr.execute('SELECT * FROM HOMICIDE_MEDIA.HOMICIDE_ARTICLE')
        # for record in csr.fetchall():
        #     print(record['article_id'], record['no_of_subs'])
            
        # csr.execute('SELECT * FROM HOMICIDE_VICTIM.VICTIM')
        # for record1 in csr.fetchall():
        #     print(record1['victim_id'], record1['article_id'])
            
        csr.execute('SELECT * FROM HOMICIDE_PERPETRATOR.PERPETRATOR')
        for record2 in csr.fetchall():
            print(record2['perpetrator_id'], record2['victim_id'])
            
        
        connection.commit()

        
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if csr is not None:
            csr.close()
        if connection is not None:
            connection.close()
            print('Database connection terminated.')
            
if __name__ == "__main__":
    connect()
