from flask import Flask, request, jsonify
import sqlite3

app = Flask(__name__)

# Function to initialize the database
def init_db():
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS locations
                 (PostalCode TEXT, KelurahanCode TEXT, KelurahanName TEXT, KecamatanCode TEXT, KecamatanName TEXT,
                  Dati2Code TEXT, Dati2Name TEXT, IsDati2Flag TEXT, MainKelurahanCode TEXT, MainKecamatanCode TEXT,
                  MainDati2Code TEXT, CityCode TEXT, CityName TEXT, ProvinceCode TEXT, ProvinceName TEXT)''')
    conn.commit()
    conn.close()

# Initialize the database
init_db()

# Route to insert data
@app.route('/insert', methods=['POST'])
def insert_data():
    data = request.json.get('Dati2Data', {}).get('Row', [])
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    for row in data:
        c.execute('''INSERT INTO locations (PostalCode, KelurahanCode, KelurahanName, KecamatanCode, KecamatanName,
                                            Dati2Code, Dati2Name, IsDati2Flag, MainKelurahanCode, MainKecamatanCode,
                                            MainDati2Code, CityCode, CityName, ProvinceCode, ProvinceName)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  (row['PostalCode'], row['KelurahanCode'], row['KelurahanName'], row['KecamatanCode'], row['KecamatanName'],
                   row['Dati2Code'], row['Dati2Name'], row['IsDati2Flag'], row['MainKelurahanCode'], row['MainKecamatanCode'],
                   row['MainDati2Code'], row['CityCode'], row['CityName'], row['ProvinceCode'], row['ProvinceName']))
    conn.commit()
    conn.close()
    return jsonify({"message": "Data inserted successfully"}), 201

# Route to fetch all data with parameter names
@app.route('/data', methods=['GET'])
def get_data():
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    c.execute('SELECT * FROM locations')
    rows = c.fetchall()
    conn.close()

    # Get column names from the database
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    c.execute('PRAGMA table_info(locations)')
    columns = c.fetchall()
    column_names = [column[1] for column in columns]  # Extract column names
    conn.close()

    # Format the response to include parameter names
    formatted_data = []
    for row in rows:
        row_dict = {}
        for i, value in enumerate(row):
            row_dict[column_names[i]] = value
        formatted_data.append(row_dict)

    return jsonify(formatted_data), 200

# Route to fetch data by Dati2Code (using request body)
@app.route('/data/by-dati2code', methods=['POST'])
def get_data_by_dati2code():
    # Get Dati2Code from the request body
    data = request.json
    if not data or 'Dati2Code' not in data:
        return jsonify({"error": "Dati2Code is required in the request body"}), 400

    dati2code = data['Dati2Code']

    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    c.execute('SELECT * FROM locations WHERE Dati2Code = ?', (dati2code,))
    rows = c.fetchall()
    conn.close()

    # Get column names from the database
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    c.execute('PRAGMA table_info(locations)')
    columns = c.fetchall()
    column_names = [column[1] for column in columns]  # Extract column names
    conn.close()

    # Format the response to include parameter names
    formatted_data = []
    for row in rows:
        row_dict = {}
        for i, value in enumerate(row):
            row_dict[column_names[i]] = value
        formatted_data.append(row_dict)

    return jsonify(formatted_data), 200

if __name__ == '__main__':
    app.run(debug=True)