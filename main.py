
import pandas as pd
import psycopg2
from sklearn.preprocessing import LabelEncoder
from sqlalchemy import create_engine, Table, Column, Integer,Float, String, MetaData


conn = psycopg2.connect(
        dbname="souvenirDB",
        user="postgres",
        password="Persona5Royal1",
        host="localhost",
        port="5432"
    )


df = pd.read_excel('data.xlsx')

df = df.rename(columns={'qtypics': 'QTypics'})
df_encoded = df.copy()



def add_to_table_and_return_dict(table, column):
    unique_values = {item: None for item in df[column].unique()}
    cursor = conn.cursor()
    for key in unique_values.keys():
        cursor.execute(f" INSERT INTO {table} (Name) VALUES ('{key}') RETURNING ID")
        row = cursor.fetchone()
        unique_values[key] = row[0]
    return unique_values


colors = add_to_table_and_return_dict('colors', 'color')
materials = add_to_table_and_return_dict('souvenirmaterials', 'material')
methods = add_to_table_and_return_dict ('applicationmethods', 'applicMetod')
print(methods)




try:

    cursor = conn.cursor()

    # 3. Insert data into the Souvenirs table
    insert_query = """
    INSERT INTO Souvenirs 
    (ShortName, Name, Description, Rating, IdCategory, IdColor, Size, IdMaterial, Weight, 
     QTopics, PicsSize, IdApplicMethod, AllCategories, DealerPrice, Price)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # 4. Iterate over the DataFrame and insert each row
    for _, row in df.iterrows():
        cursor.execute(insert_query, (
            row['shortname'],
            row['name'],
            row['description'],
            row['rating'],
            int(row['categoryid']),
            colors[row['color']],
            row['prodsize'],
            materials[row['material']],
            float(row['weight']) if pd.notna(row['weight']) else None,
            float(row['QTypics']) if pd.notna(row['QTypics']) else None,
            row['picssize'],
            methods[row['applicMetod']],
            False,
            float(row['dealerPrice']) if pd.notna(row['dealerPrice']) else None,
            float(row['price']) if pd.notna(row['price']) else None
        ))

    # 5. Commit the transaction and close the connection
    conn.commit()

except Exception as error:
    print(f"Error: {error}")
finally:
    if conn:
        cursor.close()
        conn.close()

