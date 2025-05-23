
# Board Game Data Collector

## What is This?
This project gathers and organizes detailed information about thousands of board games directly from [BoardGameGeek.com](https://boardgamegeek.com) a popular website for board game enthusiasts. It processes and stores that data into a Microsoft SQL Server.

## How Does It Work?
- **Collecting Data:** It fetches real-time information from BoardGameGeek’s public API.
- **Processing & Organizing:** Data is cleaned, structured, and prepared.
- **Storing Data:** Information is stored neatly in a database, ready for easy access and analysis.
- **Error Handling & Reliability:** The system handles errors gracefully, ensuring that data collection continues even if individual records have issues.

## Best Feautures
- **Fast & Efficient:** Uses advanced threading to process multiple games simultaneously.
- **Robust Error Management:** Automatically retries failed requests to ensure completeness.
- **Progress Tracking:** Continuously updates progress, showing estimated completion time.

## What Data Does It Capture?
- Game details: Name, publisher, designers, year published, playtime, recommended player numbers.
- User-generated content: Reviews, ratings, and community polls.
- Marketplace data: Historical and current listings, including prices and conditions.

## Technologies Used
- **Python:** Efficient data handling and processing.
- **SQL & SQLAlchemy:** Robust database interactions.
- **Pandas:** Data organization and processing.
- **BoardGameGeek API:** Real-time board game data access.
- **Microsoft SQL Server:** An SQL server that stores the collected data.

## 📁 Project Structure
- `TransformAndLoad.py`: Coordinates data collection, threading, error handling, and database updates.
- `etl_utils.py`: Contains the helper functions and core logic.
- `processed_ids.csv`: Stores processed board game IDs to save progress in the event of a crash. 
- `id_list.csv`: A list of IDs created from the downloaded boardgames_ranks.csv file
