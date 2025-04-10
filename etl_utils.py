from sqlalchemy.sql import text # Imports the text function to write raw SQL queries using SQLAlchemy.
import requests as req # Imports the requests library for making HTTP requests to get xml files from BoardGameGeek's API.
from time import sleep # Imports the sleep function to pause execution, used in delaying a retry for a querie or request.
import sqlalchemy as sqa # Imports SQLAlchemy for database connection and SQL operations.
import re # Imports the re module for working with regular expressions.
import io # Imports the io module for handling streams and in-memory file operations used to parse xml file.
import xml.etree.ElementTree as emt # Imports ElementTree for parsing and creating XML data structures.
from datetime import datetime # Imports the datetime class to parse values into datetime format.

# Make fetch data from TheBoardGameGeek using thier API, retry after a delay if unsucessful
def fetch_data(url, retries=10, delay=2):
    for attempt in range(retries):
        try:
            response = req.get(url, timeout=30, stream=True)
            response.raise_for_status()
            return response
        except req.RequestException as e:
            if attempt < retries - 1:
                sleep_time = delay * (2 ** attempt)  # exponential backoff: 2s, 4s, 8s...
                sleep(sleep_time)
            else:
                raise

# Recursively parses an XML tree into a nested dictionary, preserving text, attributes, and handling repeated tags as lists.
def parse_tree(tree):
    node_dict = {}

    # Capture text if it's non-empty (strip whitespace)
    text = (tree.text or "").strip()
    if text:
        node_dict["text"] = text

    # Capture attributes (if any)
    if tree.attrib:
        node_dict["attributes"] = dict(tree.attrib)

    # Process children
    for child in tree:
        child_tag = child.tag
        child_dict = parse_tree(child)  # Recursively parse the child

        # If we already have this tag, we need to store it as a list
        if child_tag in node_dict:
            # If it's not already a list, convert it to a list
            if not isinstance(node_dict[child_tag], list):
                node_dict[child_tag] = [node_dict[child_tag]]
            node_dict[child_tag].append(child_dict)
        else:
            # First time seeing this tag
            node_dict[child_tag] = child_dict

    return node_dict

# Load data into the tables of many to many relationships given a list of data and table names
def many_to_many_table_load(list, table_name, plural, connection, board_game_id):
    if list:
        query = text(f"""
        MERGE INTO {plural} AS target
        USING (
            SELECT
                :id AS id,
                :name AS name
        ) AS source
        ON target.id = source.id
        WHEN MATCHED THEN
            UPDATE SET
                target.id = source.id,
                target.name = source.name
        WHEN NOT MATCHED THEN
            INSERT (
                id,
                name
            )
            VALUES (
                :id,
                name
            );
        """)

        junction_query = text(f"""
        MERGE INTO board_game_has_{table_name} AS target
        USING (
            SELECT
                :board_game_id AS board_game_id,
                :{table_name}_id AS {table_name}_id
        ) AS source
        ON target.board_game_id = source.board_game_id
            AND target.{table_name}_id = source.{table_name}_id
        WHEN MATCHED THEN
            UPDATE SET
                target.board_game_id = source.board_game_id,
                target.{table_name}_id = source.{table_name}_id
        WHEN NOT MATCHED THEN
            INSERT (
                board_game_id,
                {table_name}_id
            )
            VALUES (
                :board_game_id,
                :{table_name}_id
            );
        """)


        if isinstance(list, dict):   
            id = list.get("attributes", {}).get("objectid")
            name = list.get("text")
            if id:
                params = {
                    "id": id,
                    "name": name
                }
                juction_params = {
                    "board_game_id": board_game_id,
                    f"{table_name}_id": id
                }
                safe_execute(connection, query, params)
                safe_execute(connection, junction_query, juction_params)
        else:
            for item in list:
                id = item.get("attributes", {}).get("objectid")
                name = item.get("text")
                if id:
                    params = {
                        "id": id,
                        "name": name
                    }
                    juction_params = {
                        "board_game_id": board_game_id,
                        f"{table_name}_id": id
                    }
                    safe_execute(connection, query, params)
                    safe_execute(connection, junction_query, juction_params)

# Turn a string into a float value
def parse_float(value_str):
    try:
        return float(value_str)
    except (TypeError, ValueError):
        return None

# Extracts the text value from an element if it's a dictionary or returns the string directly.
def get_single_value(element):
    if not element:
        return None
    if isinstance(element, dict):
        return element.get("text")
    if isinstance(element, str):
        return element
    return None

# Returns the primary name from a list of name entries, falling back to the first if no primary is marked.
def get_primary_name(name_list):
    if not name_list:
        return None
    if isinstance(name_list, list):
        for entry in name_list:
            if isinstance(entry, dict) and entry.get("attributes", {}).get("primary") == "true":
                return entry.get("text")
        # Fallback to the first element's text if no primary is found
        first = name_list[0]
        return first.get("text") if isinstance(first, dict) else first
    elif isinstance(name_list, dict):
        return name_list.get("text")
    return None

# Extracts the recommended number of players from a poll summary.
def get_recommended_players(poll_summary):
    if not poll_summary or not isinstance(poll_summary, dict):
        return None
    for result in poll_summary.get("result", []):
        if result.get("attributes", {}).get("name") == "recommmendedwith":
            return result.get("attributes", {}).get("value")
    return None

# Retrieves the best number of players from a poll summary.
def get_best_number_of_players(poll_summary):
    if not poll_summary or not isinstance(poll_summary, dict):
        return None
    for result in poll_summary.get("result", []):
        if result.get("attributes", {}).get("name") == "bestwith":
            return result.get("attributes", {}).get("value")
    return None

# Returns the total votes from the "User Suggested Number of Players" poll.
def get_suggested_players_votes(poll_list):
    if not poll_list:
        return None
    for entry in poll_list:
        attributes = entry.get("attributes", {})
        if attributes.get("title") == "User Suggested Number of Players":
            return attributes.get("totalvotes")
    return None

# Retrieves the total votes from the "User Suggested Player Age" poll.
def get_suggested_age_votes(poll_list):
    if not poll_list:
        return None
    for entry in poll_list:
        attributes = entry.get("attributes", {})
        if attributes.get("title") == "User Suggested Player Age":
            return attributes.get("totalvotes")
    return None

# Debug query: DO NOT CALL UNLESS YOU WANT TO DELETE ALL DATA IN YOUR DATABASE AND START OVER!!!
def nuke_database():
    engine = sqa.create_engine('mssql+pyodbc://@localhost/board_games?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server')
    connection = engine.connect()
    transaction = connection.begin()

    nuke_query = text("""
    -- Disable all constraints in all tables
    EXEC sp_MSforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT ALL';

    -- Delete all data from all tables
    EXEC sp_MSforeachtable 'DELETE FROM ?';

    -- Reset identity only on tables that have an identity column
    DECLARE @tablename NVARCHAR(256);
    DECLARE table_cursor CURSOR FOR 
    SELECT QUOTENAME(s.name) + '.' + QUOTENAME(t.name)
    FROM sys.tables t
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE EXISTS (
        SELECT 1 FROM sys.columns c 
        WHERE c.object_id = t.object_id AND c.is_identity = 1
    );
    OPEN table_cursor;
    FETCH NEXT FROM table_cursor INTO @tablename;
    WHILE @@FETCH_STATUS = 0
    BEGIN
    -- Enclose @tablename in single quotes in the dynamic SQL:
    EXEC ('DBCC CHECKIDENT(''' + @tablename + ''', RESEED, 0)');
    FETCH NEXT FROM table_cursor INTO @tablename;
    END;
    CLOSE table_cursor;
    DEALLOCATE table_cursor;


    -- Re-enable all constraints in all tables
    EXEC sp_MSforeachtable 'ALTER TABLE ? WITH CHECK CHECK CONSTRAINT ALL';
    """)
    safe_execute(connection, nuke_query, None)
    transaction.commit()
    connection.close() 

# Executes the query, if locked by other thread, retry after a delay
def safe_execute(connection, query, params, retries=6, delay=1):
    for attempt in range(retries):
        try:
            connection.execute(query, params)
            return  # success
        except Exception as e:
            if attempt < retries - 1:
                sleep_time = delay * (2 ** attempt)  # exponential backoff: 2s, 4s, 8s...
                sleep(sleep_time)
                continue  # retry
            else:
                raise

# Main Function, get API request, parse and load data into the SQL server
def process_board_game(id):
    try:
        # Connect to Database
        engine = sqa.create_engine(f'mssql+pyodbc://@localhost/board_games?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server')
        connection = engine.connect()
        transaction = connection.begin()

        # Get API respnse
        url = f"https://api.geekdo.com/xmlapi/boardgame/{id}?&stats=1&comments=1&historical=1&pricehistory=1&marketplace=1"
        response = fetch_data(url)
        if response is None:
            print(f"Got no response for ID: {id}")
            return 
        xml_content = response.content.decode("utf-8", errors="replace")
        xml_content = re.sub(r'[\x00-\x08\x0B-\x0C\x0E-\x1F]', '', xml_content)  # Clean the XML
        xml_stream = io.StringIO(xml_content)

        # Stream & parse the XML
        for event, elem in emt.iterparse(xml_stream, events=("end",)):
            if elem.tag == "boardgame":
                data = parse_tree(elem)
                elem.clear()  # Free memory

                # Parse and upload data to board_games table
                board_game_params = {
                    "id": id,
                    "name": get_primary_name(data.get("name")),
                    "year_published": get_single_value(data.get("yearpublished")),
                    "min_players": get_single_value(data.get("minplayers")),
                    "max_players": get_single_value(data.get("maxplayers")),
                    "playing_time": get_single_value(data.get("playingtime")),
                    "min_play_time": get_single_value(data.get("minplaytime")),
                    "max_play_time": get_single_value(data.get("maxplaytime")),
                    "age": get_single_value(data.get("age")),
                    "description": get_single_value(data.get("description")),
                    "thumbnail": get_single_value(data.get("thumbnail")),
                    "image": get_single_value(data.get("image")),
                    "users_rated": get_single_value(data.get("statistics", {}).get("ratings", {}).get("usersrated")),
                    "average": get_single_value(data.get("statistics", {}).get("ratings", {}).get("average")),
                    "bayes_average": get_single_value(data.get("statistics", {}).get("ratings", {}).get("bayesaverage")),
                    "standard_deviation": get_single_value(data.get("statistics", {}).get("ratings", {}).get("stddev")),
                    "median": get_single_value(data.get("statistics", {}).get("ratings", {}).get("median")),
                    "owned": get_single_value(data.get("statistics", {}).get("ratings", {}).get("owned")),
                    "trading": get_single_value(data.get("statistics", {}).get("ratings", {}).get("trading")),
                    "wanting": get_single_value(data.get("statistics", {}).get("ratings", {}).get("wanting")),
                    "wishing": get_single_value(data.get("statistics", {}).get("ratings", {}).get("wishing")),
                    "number_of_comments": get_single_value(data.get("statistics", {}).get("ratings", {}).get("numcomments")),
                    "number_of_weights": get_single_value(data.get("statistics", {}).get("ratings", {}).get("numweights")),
                    "average_weight": get_single_value(data.get("statistics", {}).get("ratings", {}).get("averageweight")),
                    "recommended_players": get_recommended_players(data.get("poll-summary")),
                    "best_number_of_players": get_best_number_of_players(data.get("poll-summary")),
                    "suggested_players_votes": get_suggested_players_votes(data.get("poll")),
                    "suggested_age_votes": get_suggested_age_votes(data.get("poll"))
                }
                
                board_game_query = text("""
                MERGE INTO board_games AS target
                USING (
                    SELECT
                        :id AS id,
                        :name AS name,
                        :year_published AS year_published,
                        :min_players AS min_players,
                        :max_players AS max_players,
                        :playing_time AS playing_time,
                        :min_play_time AS min_play_time,
                        :max_play_time AS max_play_time,
                        :age AS age,
                        :description AS description,
                        :thumbnail AS thumbnail,
                        :image AS image,
                        :users_rated AS users_rated,
                        :average AS average,
                        :bayes_average AS bayes_average,
                        :standard_deviation AS standard_deviation,
                        :median AS median,
                        :owned AS owned,
                        :trading AS trading,
                        :wanting AS wanting,
                        :wishing AS wishing,
                        :number_of_comments AS number_of_comments,
                        :number_of_weights AS number_of_weights,
                        :average_weight AS average_weight,
                        :recommended_players AS recommended_players,
                        :best_number_of_players AS best_number_of_players,
                        :suggested_players_votes AS suggested_players_votes,
                        :suggested_age_votes AS suggested_age_votes
                ) AS source
                ON target.id = source.id
                WHEN MATCHED THEN
                    UPDATE SET
                        target.name = source.name,
                        target.year_published = source.year_published,
                        target.min_players = source.min_players,
                        target.max_players = source.max_players,
                        target.playing_time = source.playing_time,
                        target.min_play_time = source.min_play_time,
                        target.max_play_time = source.max_play_time,
                        target.age = source.age,
                        target.description = source.description,
                        target.thumbnail = source.thumbnail,
                        target.image = source.image,
                        target.users_rated = source.users_rated,
                        target.average = source.average,
                        target.bayes_average = source.bayes_average,
                        target.standard_deviation = source.standard_deviation,
                        target.median = source.median,
                        target.owned = source.owned,
                        target.trading = source.trading,
                        target.wanting = source.wanting,
                        target.wishing = source.wishing,
                        target.number_of_comments = source.number_of_comments,
                        target.number_of_weights = source.number_of_weights,
                        target.average_weight = source.average_weight,
                        target.recommended_players = source.recommended_players,
                        target.best_number_of_players = source.best_number_of_players,
                        target.suggested_players_votes = source.suggested_players_votes,
                        target.suggested_age_votes = source.suggested_age_votes
                WHEN NOT MATCHED THEN
                    INSERT (
                        id,
                        name,
                        year_published,
                        min_players,
                        max_players,
                        playing_time,
                        min_play_time,
                        max_play_time,
                        age,
                        description,
                        thumbnail,
                        image,
                        users_rated,
                        average,
                        bayes_average,
                        standard_deviation,
                        median,
                        owned,
                        trading,
                        wanting,
                        wishing,
                        number_of_comments,
                        number_of_weights,
                        average_weight,
                        recommended_players,
                        best_number_of_players,
                        suggested_players_votes,
                        suggested_age_votes
                    )
                    VALUES (
                        :id,
                        :name,
                        :year_published,
                        :min_players,
                        :max_players,
                        :playing_time,
                        :min_play_time,
                        :max_play_time,
                        :age,
                        :description,
                        :thumbnail,
                        :image,
                        :users_rated,
                        :average,
                        :bayes_average,
                        :standard_deviation,
                        :median,
                        :owned,
                        :trading,
                        :wanting,
                        :wishing,
                        :number_of_comments,
                        :number_of_weights,
                        :average_weight,
                        :recommended_players,
                        :best_number_of_players,
                        :suggested_players_votes,
                        :suggested_age_votes
                    );
                """)

                safe_execute(connection, board_game_query, board_game_params)
                
                # Parse and upload data to all tables that have many to many relationships with the board_games table.
                try:
                    many_to_many_table_load(data["boardgameaccessory"], "accessory", "accessories", connection, id)
                except:
                    pass

                try:
                    many_to_many_table_load(data["boardgameartist"], "artist", "artists", connection, id)
                except:
                    pass

                try:
                    many_to_many_table_load(data["boardgamecategory"], "category", "categories", connection, id)
                except:
                    pass

                try:
                    many_to_many_table_load(data["boardgamedesigner"], "designer", "designers", connection, id)
                except:
                    pass

                try:
                    many_to_many_table_load(data["boardgamefamily"], "family", "families", connection, id)
                except:
                    pass

                try:
                    many_to_many_table_load(data["boardgamehonor"], "honor", "honors", connection, id)
                except:
                    pass

                try:
                    many_to_many_table_load(data["boardgamemechanic"], "mechanic", "mechanics", connection, id)
                except:
                    pass

                try:
                    many_to_many_table_load(data["boardgamepublisher"], "publisher", "publishers", connection, id)
                except:
                    pass

                try:
                    many_to_many_table_load(data["boardgamesubdomain"], "subdomain", "subdomains", connection, id)
                except:
                    pass

                try:
                    many_to_many_table_load(data["boardgameversion"], "version", "versions", connection, id)
                except:
                    pass

                # Parse and upload data to the comments table.
                comments = comments = data.get("comment")
                if comments:
                    query = text(f"""
                    MERGE INTO comments AS target
                    USING (
                        SELECT
                            CAST(:body AS NVARCHAR(MAX)) AS body,
                            :username AS username,
                            :rating AS rating,
                            :board_game_id AS board_game_id
                    ) AS source
                    ON (
                        target.board_game_id = source.board_game_id
                        AND target.username   = source.username
                        AND CAST(target.body AS VARCHAR(MAX)) = source.body
                        AND target.rating     = source.rating
                        )
                    WHEN NOT MATCHED THEN
                        INSERT (
                            body,
                            username,
                            rating,
                            board_game_id
                        )
                        VALUES (
                            :body,
                            :username,
                            :rating,
                            :board_game_id
                        );
                    """)
                    if isinstance(comments, dict):   
                        body = comments.get("text")
                        username = comments.get("attributes", {}).get("username")
                        rating = comments.get("attributes", {}).get("rating")
                        if rating == "N/A":
                            rating = None
                        params = {
                            "body": body,
                            "username": username,
                            "rating": rating,
                            "board_game_id": data.get("attributes", {}).get("objectid")
                        }
                        safe_execute(connection, query, params)
                    else:
                        for comment in comments:
                            body = comment.get("text")
                            username = comment.get("attributes", {}).get("username")
                            rating = parse_float(comment.get("attributes", {}).get("rating"))
                            params = {
                                "body": body,
                                "username": username,
                                "rating": rating,
                                "board_game_id": data.get("attributes", {}).get("objectid")
                            }
                            safe_execute(connection, query, params)
                current_listings = data.get("marketplacelistings", {}).get("listing")
                if current_listings:

                    query = text("""
                    MERGE INTO current_listings AS target
                    USING (
                        SELECT
                            :list_date AS list_date,
                            :price AS price,
                            :currency AS currency,
                            :condition AS condition,
                            :note AS note,
                            :link AS link,
                            :board_game_id AS board_game_id
                    ) AS source
                    ON (
                        target.list_date = source.list_date
                        AND target.price = source.price
                        AND target.currency = source.currency
                        AND target.condition = source.condition
                        AND target.board_game_id = source.board_game_id
                        )
                    WHEN NOT MATCHED THEN
                        INSERT (
                            list_date,
                            price,
                            currency,
                            condition,
                            note,
                            link,
                            board_game_id
                        )
                        VALUES (
                            :list_date,
                            :price,
                            :currency,
                            :condition,
                            :note,
                            :link,
                            :board_game_id
                        );
                    """)
                    if isinstance(current_listings, dict):   
                        date_str = current_listings.get("listdate").get("text")
                        dt = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %z")
                        list_date = dt.strftime("%Y-%m-%d %H:%M:%S")

                        price = parse_float(current_listings.get("price").get("text"))
                        currency = current_listings.get("price").get("attributes", {}).get("currency")
                        condition = current_listings.get("condition").get("text")
                        note = current_listings.get("notes").get("text")
                        try:
                            link = current_listings.get(link).get("attributes", {}).get("href")
                        except:
                            link = None
                        board_game_id = data.get("attributes", {}).get("objectid")
                        params = {
                            "list_date": list_date,
                            "price": price,
                            "currency": currency,
                            "condition": condition,
                            "note": note,
                            "link": link,
                            "board_game_id": board_game_id
                        }
                        safe_execute(connection, query, params)
                    else:
                        for listing in current_listings:
                            date_str = listing.get("listdate").get("text")
                            dt = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %z")
                            list_date = dt.strftime("%Y-%m-%d %H:%M:%S")

                            price = parse_float(listing.get("price").get("text"))
                            currency = listing.get("price").get("attributes", {}).get("currency")
                            condition = listing.get("condition").get("text")
                            note = listing.get("notes").get("text")
                            try:
                                link = listing.get("link").get("attributes", {}).get("href")
                            except:
                                link = None
                            board_game_id = data.get("attributes", {}).get("objectid")
                            params = {
                                "list_date": list_date,
                                "price": price,
                                "currency": currency,
                                "condition": condition,
                                "note": note,
                                "link": link,
                                "board_game_id": board_game_id
                            }
                            safe_execute(connection, query, params)

                # Parse and upload data to the previous_listings table.
                previous_listings = data.get("marketplacehistory", {}).get("listing")
                if previous_listings:
                    query = text("""
                    MERGE INTO previous_listings AS target
                    USING (
                        SELECT
                            :list_date AS list_date,
                            :sale_date AS sale_date,
                            :price AS price,
                            :currency AS currency,
                            :condition AS condition,
                            :note AS note,
                            :board_game_id AS board_game_id
                    ) AS source
                    ON (
                        target.list_date = source.list_date
                        AND target.sale_date = source.sale_date
                        AND target.price = source.price
                        AND target.currency = source.currency
                        AND target.condition = source.condition
                        AND target.board_game_id = source.board_game_id
                        )
                    WHEN NOT MATCHED THEN
                        INSERT (
                            list_date,
                            sale_date,
                            price,
                            currency,
                            condition,
                            note,
                            board_game_id
                        )
                        VALUES (
                            :list_date,
                            :sale_date,
                            :price,
                            :currency,
                            :condition,
                            :note,
                            :board_game_id
                        );
                    """)
                    if isinstance(previous_listings, dict):   
                        date_str = previous_listings.get("listdate", {}).get("text")
                        dt = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %z")
                        list_date = dt.strftime("%Y-%m-%d %H:%M:%S")

                        date_str = previous_listings.get("saledate", {}).get("text")
                        dt = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %z")
                        sale_date = dt.strftime("%Y-%m-%d %H:%M:%S")

                        price = parse_float(previous_listings.get("price", {}).get("text"))
                        currency = previous_listings.get("price", {}).get("attributes", {}).get("currency")
                        condition = previous_listings.get("condition", {}).get("text")
                        note = previous_listings.get("notes", {}).get("text")
                        board_game_id = data.get("attributes", {}).get("objectid")
                        params = {
                            "list_date": list_date,
                            "sale_date": sale_date,
                            "price": price,
                            "currency": currency,
                            "condition": condition,
                            "note": note,
                            "board_game_id": board_game_id
                        }
                        safe_execute(connection, query, params)
                    else:
                        for listing in previous_listings:
                            date_str = listing.get("listdate").get("text")
                            dt = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %z")
                            list_date = dt.strftime("%Y-%m-%d %H:%M:%S")

                            date_str = listing.get("saledate").get("text")
                            dt = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %z")
                            sale_date = dt.strftime("%Y-%m-%d %H:%M:%S")

                            price = parse_float(listing.get("price", {}).get("text"))
                            currency = listing.get("price", {}).get("attributes", {}).get("currency")
                            condition = listing.get("condition", {}).get("text")
                            note = listing.get("notes", {}).get("text")
                            board_game_id = data.get("attributes", {}).get("objectid")
                            params = {
                                "list_date": list_date,
                                "sale_date": sale_date,
                                "price": price,
                                "currency": currency,
                                "condition": condition,
                                "note": note,
                                "board_game_id": board_game_id
                            }
                            safe_execute(connection, query, params)

                # Parse and upload data to the ranks table.
                rankings = data.get("statistics", {}).get("ratings", {}).get("ranks", {}).get("rank")
                if rankings:
                    query = text(f"""
                    MERGE INTO ranks AS target
                    USING (
                        SELECT
                            :type AS type,
                            :name AS name,
                            :friendly_name AS friendly_name,
                            :value AS value,
                            :bayes_average AS bayes_average,
                            :board_game_id AS board_game_id,
                            :rank_id AS rank_id
                    ) AS source
                    ON (
                        target.type = source.type
                        AND target.name = source.name
                        AND target.friendly_name = source.friendly_name
                        AND target.value = source.value
                        AND target.bayes_average = source.bayes_average
                        AND target.board_game_id = source.board_game_id
                        AND target.rank_id = source.rank_id
                        )
                    WHEN NOT MATCHED THEN
                        INSERT (
                            type,
                            name,
                            friendly_name,
                            value,
                            bayes_average,
                            board_game_id,
                            rank_id
                        )
                        VALUES (
                            :type,
                            :name,
                            :friendly_name,
                            :value,
                            :bayes_average,
                            :board_game_id,
                            :rank_id
                        );
                    """)
                    if isinstance(rankings, dict):   
                        type = rankings.get("attributes", {}).get("type")
                        name = rankings.get("attributes", {}).get("name")
                        friendly_name = rankings.get("attributes", {}).get("friendlyname")
                        value = rankings.get("attributes", {}).get("value")
                        if (value == "Not Ranked"):
                            value = None
                        bayes_average = rankings.get("attributes", {}).get("bayesaverage")
                        if (bayes_average == "Not Ranked"):
                            bayes_average = None
                        rank_id = rankings.get("attributes", {}).get("id")
                        params = {
                            "type": type,
                            "name": name,
                            "friendly_name": friendly_name,
                            "value": value,
                            "bayes_average": bayes_average,
                            "board_game_id": data.get("attributes", {}).get("objectid"),
                            "rank_id": rank_id
                        }
                        safe_execute(connection, query, params)
                    else:
                        for rank in rankings:
                            type = rank.get("attributes", {}).get("type")
                            name = rank.get("attributes", {}).get("name")
                            friendly_name = rank.get("attributes", {}).get("friendlyname")
                            value = rank.get("attributes", {}).get("value")
                            if (value == "Not Ranked"):
                                value = None
                            bayes_average = rank.get("attributes", {}).get("bayesaverage")
                            if (bayes_average == "Not Ranked"):
                                bayes_average = None
                            board_game_id = data.get("attributes", {}).get("objectid")
                            rank_id = rank.get("attributes", {}).get("id")
                            params = {
                                "type": type,
                                "name": name,
                                "friendly_name": friendly_name,
                                "value": value,
                                "bayes_average": bayes_average,
                                "board_game_id": board_game_id,
                                "rank_id": rank_id
                            }
                            safe_execute(connection, query, params)

                # Parse and upload data to the sugested_players table.
                polls = data.get("poll")
                if polls:
                    for poll in polls:
                        if poll.get("attributes", {}).get("name") == "suggested_numplayers":
                            player_polls = poll.get("results")
                    if player_polls:
                        query = text(f"""
                        MERGE INTO suggested_players_poll AS target
                        USING (
                            SELECT
                                :best AS best,
                                :recommended AS recommended,
                                :not_recommended AS not_recommended,
                                :number_of_players AS number_of_players,
                                :board_game_id AS board_game_id
                        ) AS source
                        ON (
                            target.board_game_id = source.board_game_id
                            AND target.number_of_players = source.number_of_players
                            )
                        WHEN MATCHED THEN
                            UPDATE SET
                                target.best = source.best,
                                target.recommended = source.recommended,
                                target.not_recommended = source.not_recommended
                        WHEN NOT MATCHED THEN
                            INSERT (
                                best,
                                recommended,
                                not_recommended,
                                number_of_players,
                                board_game_id
                            )
                            VALUES (
                                :best,
                                :recommended,
                                :not_recommended,
                                :number_of_players,
                                :board_game_id
                            );
                        """)
                        if isinstance(player_polls, dict):
                            number_of_players = player_polls.get("attributes", {}).get("numplayers")
                            votes = player_polls.get("result")
                            if (votes):
                                for vote in votes:
                                    if vote.get("attributes", {}).get("value") == "Best":
                                        best = vote.get("attributes", {}).get("numvotes")
                                    elif vote.get("attributes", {}).get("value") == "Recommended":
                                        recommended = vote.get("attributes", {}).get("numvotes")
                                    elif vote.get("attributes", {}).get("value") == "Not Recommended":
                                        not_recommended = vote.get("attributes", {}).get("numvotes")
                            else:
                                best = None
                                recommended = None
                                not_recommended = None
                            board_game_id = id
                            params = {
                                "best": best,
                                "recommended": recommended,
                                "not_recommended": not_recommended,
                                "number_of_players": number_of_players,
                                "board_game_id": board_game_id,
                            }
                            safe_execute(connection, query, params)
                        else:
                            for poll in player_polls:
                                number_of_players = poll.get("attributes", {}).get("numplayers")
                                votes = poll.get("result")
                                for vote in votes:
                                    if vote.get("attributes", {}).get("value") == "Best":
                                        best = vote.get("attributes", {}).get("numvotes")
                                    elif vote.get("attributes", {}).get("value") == "Recommended":
                                        recommended = vote.get("attributes", {}).get("numvotes")
                                    elif vote.get("attributes", {}).get("value") == "Not Recommended":
                                        not_recommended = vote.get("attributes", {}).get("numvotes")
                                board_game_id = data.get("attributes", {}).get("objectid")
                                params = {
                                    "best": best,
                                    "recommended": recommended,
                                    "not_recommended": not_recommended,
                                    "number_of_players": number_of_players,
                                    "board_game_id": board_game_id,
                                }
                                safe_execute(connection, query, params)

                # Parse and upload data to the suggested_age_poll table.  
                polls = data.get("poll")
                if polls:
                    for poll in polls:
                        if poll.get("attributes", {}).get("name") == "suggested_playerage":
                            age_polls = poll.get("results", {}).get("result")
                    if age_polls:
                        query = text(f"""
                        MERGE INTO suggested_age_poll AS target
                        USING (
                            SELECT
                                :age AS age,
                                :votes AS votes,
                                :board_game_id AS board_game_id
                        ) AS source
                        ON (
                            target.board_game_id = source.board_game_id
                            AND target.age = source.age
                            )
                        WHEN MATCHED THEN
                            UPDATE SET
                                target.votes = source.votes
                        WHEN NOT MATCHED THEN
                            INSERT (
                                age,
                                votes,
                                board_game_id
                            )
                            VALUES (
                                :age,
                                :votes,
                                :board_game_id
                            );
                        """)
                        for poll in age_polls:
                            age = poll.get("attributes", {}).get("value")
                            votes = poll.get("attributes", {}).get("numvotes")
                            board_game_id = data.get("attributes", {}).get("objectid")
                            params = {
                                "age": age,
                                "votes": votes,
                                "board_game_id": board_game_id,
                            }
                            safe_execute(connection, query, params)
  
        # If all queries are sucessfull, commit the transaction and close the connection
        transaction.commit()
        connection.close() 
    except:
        # If timeout from requests, queries, or a unexpected error occurs, undo all queries and close the connection
        transaction.rollback()
        connection.close()
        raise