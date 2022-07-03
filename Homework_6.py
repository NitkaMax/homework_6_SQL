import psycopg2

def drop_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            DROP TABLE Phones;
            DROP TABLE Clients;
            """)
    return



def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Clients(
                id SERIAL PRIMARY KEY,
                first_name VARCHAR (40),
                last_name VARCHAR (80),
                email VARCHAR (60)
                );
                """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Phones(
                clients_id INTEGER NOT NULL REFERENCES Clients(id),
                phone_number BIGINT, 
                CONSTRAINT pk_pn PRIMARY KEY (clients_id, phone_number)
            );
            """)
        conn.commit()
    return

def add_client(conn, first_name, last_name, email, phones=None, clients_id=None):
    with conn.cursor() as cur:
        if clients_id == None:
            sql_request_text = """
            INSERT INTO Clients(first_name, last_name email) 
            VALUES (%s, %s, %s)
            """
            params = (first_name, last_name, email)
        else:
            sql_request_text = """
            INSERT INTO Clients(id, first_name, last_name, email)
            VALUES (%s, %s, %s,%s)
            ON CONFLICT (id) DO UPDATE SET first_name = excluded.first_name, last_name = excluded.last_name, email = excluded.email
            RETURNING id; 
            """
            params = (clients_id, first_name, last_name, email)

        cur.execute(sql_request_text,params)
        cur_clients_id = cur.fetchone()

        if phones != None:
            for phone in phones:
                cur.execute ("""
                INSERT INTO phones(clients_id, phone_number) VALUES(%s, %s)
                ON CONFLICT (clients_id, phone_number) DO NOTHING;
                """, (cur_clients_id, phone))
        conn.commit()
    return

def add_phone(conn, clients_id, phone, no_commit=False):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO Phones (clients_id, phone_number)
        VALUES (%s, %s)
        ON CONFLICT (clients_id, phone_number) DO NOTHING;
        """, (clients_id, phone))
        if no_commit != True:
            conn.commit()
    return

def change_client(conn, clients_id, first_name=None, last_name=None, email=None, phones=None):
    with conn.cursor() as cur:
        values_text = []
        values = []
        if first_name != None:
            values_text.append ('first_name')
            values.append(first_name)
        if last_name != None:
            values_text.append('last_name')
            values.append(last_name)
        if email != None:
            values_text.append ('email')
            values.append(email)

        values.append(clients_id)

        sql_request_text = f"""
                UPDATE clients SET {",".join ([x + " = %s" for x in values_text])}
                WHERE id = %s
                """
        print (sql_request_text)

        cur.execute(sql_request_text, values)

        if phones != None:
            cur.execute ("""
            DELETE FROM phones
            WHERE clients_id = %s;
            """, (clients_id,))
            for phone_number in phones:
                add_phone(conn=conn, clients_id=clients_id, phone=phone_number, no_commit=True)
        conn.commit()
    return


def delete_phone(conn, clients_id, phones):
    with conn.cursor() as cur:
        cur.execute ("""
        DELETE FROM phones
        WHERE clients_id = %s AND phone_number = %s;
        """, (clients_id, phones))
        conn.commit()
    return

def delete_client(conn, clients_id):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM phones
        WHERE clients_id = %s;
        """, (clients_id,))
        cur.execute ("""
        DELETE FROM clients
        WHERE id = %s;
        """, (clients_id,))
        conn.commit()
    return

def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    
    conditions = []
    params = []

    if first_name !=None:
        conditions.append('cl.first_name')
        params.append(first_name)
    if last_name != None:
        conditions.append('cl.last_name')
        params.append(last_name)
    if email !=None:
        conditions.append('cl.email')
        params.append(email)
    if phone != None:
        conditions.append('ph.phone_number')
        params.append(phone)

    sql_request_text = f"""
            SELECT * FROM clients cl
            LEFT JOIN phones ph ON cl.id = ph.clients_id
            WHERE {" AND ".join([x + " = %s" for x in conditions])}
            """
    print(sql_request_text)

    with conn.cursor() as cur:
        cur.execute (sql_request_text, params)
        print (cur.fetchall())

    return

if __name__ == '__main__':
    with psycopg2.connect(database="clients_db", user="postgres", password="") as conn:
        with conn.cursor() as cur:
    
            drop_table(conn)
            create_db(conn)

            add_client(conn, clients_id=1, first_name='Steven', last_name='Gerard', email='SteveG@icloud.com', phones=['+79049884516', '+7892246341245','+86540523652'])
            add_client(conn, clients_id=2, first_name='Frank', last_name='Lampard', email='FrankyCranky@icloud.com', phones=['+79086138797', '+0072659463784'])
            add_client(conn, clients_id=3, first_name='Ashley', last_name='Cole', email='AshCool@icloud.com', phones=['+79663669467', '+0016439761548','+79046483194','+0049640521145'])
            add_client(conn, clients_id=4, first_name='Wayne', last_name='Rooney', email='RooneySlooney@icloud.com', phones=['+79994664949', '+0084697891534','+0062468314628'])


            add_phone(conn,clients_id=3, phone='+0024567891594')
            add_phone(conn,clients_id=2, phone='+0090860456981')

            change_client(conn, clients_id=2, last_name='Ribery', email='Frankfromfrance@mail.ru', phones=['+79054987615'])
            find_client(conn, last_name='Cole')

            delete_phone(conn, clients_id=4, phones='+79994664949')
            delete_client(conn, clients_id=4)

            find_client(conn, first_name='Steven')
            find_client(conn, last_name='Ashley', phone='+0016439761548')

            print ('End')
    
    conn.close()
