import os
import snowflake.connector
import gzip

# Snowflake configurations
## If this was a production application, these would be stored in a secure location.
SNOWFLAKE_ACCOUNT = 'xkb71872.us-east-1'
SNOWFLAKE_USER = 'INGEST_USER'
SNOWFLAKE_PASSWORD = 'FiNdErymNe'
SNOWFLAKE_WAREHOUSE = 'INGEST_WAREHOUSE'
SNOWFLAKE_DATABASE = 'INGEST_DATABASE'
SNOWFLAKE_SCHEMA = 'IOWA_LIQUOR_SALES'
SNOWFLAKE_STAGE = 'STAGE__IOWA_LIQUOR_SALES'

# File configurations
CHUNK_SIZE = 50 * 1024 * 1024  # Size in bytes, currently set to 50MB


def gzip_file(input_path, output_path):
    with open(input_path, 'rb') as src, gzip.open(output_path, 'wb') as dst:
        dst.writelines(src)


def test_connection():
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        conn.close()
        print("Successfully connected to Snowflake.")
        return True
    except Exception as e:
        print("Failed to connect to Snowflake. Error:", e)
        return False


def main(file_path):
    print("Testing connection to Snowflake...")
    if not test_connection():
        print("Connection test failed. Exiting.")
        return
    else:
        print("Connection test successful.")


    print("Establishing connection to Snowflake...")
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )

    # Initiate cursor
    cursor = conn.cursor()
    ## Explicitly set the database and schema in the session
    cursor.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
    cursor.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")

    file_size = os.path.getsize(file_path)
    total_chunks = -(-file_size // CHUNK_SIZE)  # Calculate total chunks using ceiling division

    if file_size < CHUNK_SIZE:
        print(f"Compressing {file_path}...")
        gzipped_file_path = f"{file_path}.gz"
        gzip_file(file_path, gzipped_file_path)

        print(f"Uploading {gzipped_file_path} to Snowflake stage...")
        put_cmd = f"PUT file://{gzipped_file_path} @{SNOWFLAKE_STAGE}"
        cursor.execute(put_cmd)
        os.remove(gzipped_file_path)
    else:
        print(f"File is large. Splitting {file_path} into {total_chunks} chunks and uploading...")
        with open(file_path, 'rb') as f:
            part_num = 1
            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break

                chunk_file_path = f"/tmp/part_{part_num}.csv"
                gzipped_chunk_path = f"{chunk_file_path}.gz"
                
                print(f"Compressing chunk {part_num}/{total_chunks}...")
                with open(chunk_file_path, 'wb') as chunk_file:
                    chunk_file.write(chunk)

                gzip_file(chunk_file_path, gzipped_chunk_path)
                
                print(f"Uploading chunk {part_num}/{total_chunks} to Snowflake stage...")
                put_cmd = f"PUT file://{gzipped_chunk_path} @{SNOWFLAKE_STAGE}/part_{part_num}.csv.gz"
                cursor.execute(put_cmd)

                os.remove(chunk_file_path)
                os.remove(gzipped_chunk_path)
                part_num += 1

    cursor.close()
    conn.close()
    print("File upload completed successfully!")

if __name__ == "__main__":
    option = input("Choose an option:\n1. Test Snowflake Connection\n2. Upload CSV to Snowflake\nEnter 1 or 2: ")
    if option == "1":
        test_connection()
    elif option == "2":
        path = input("Enter the path of your CSV file: ")
        main(path)
    else:
        print("Invalid option.")
