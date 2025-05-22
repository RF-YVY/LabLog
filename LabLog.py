import tkinter as tk
from tkinter import ttk, filedialog, messagebox, simpledialog, scrolledtext
import sqlite3
import os
import io
import time
from datetime import datetime, date as datetime_date # For isinstance check
from PIL import Image, ImageTk
import shutil
import logging
import hashlib # For password hashing
import secrets # For generating salt
import sys # For exiting gracefully
import threading # For running long tasks in background
import queue # For inter-thread communication


# --- ReportLab ---
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image as ReportLabImage, PageBreak
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.pagesizes import letter, landscape
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_CENTER, TA_LEFT # For text alignment

# --- Mapping & Geocoding ---
import tkintermapview
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable

# --- Graphing ---
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import pandas as pd

# --- Calendar and Excel ---
from tkcalendar import DateEntry
import openpyxl # For .xlsx export (pandas uses it)


# --- Constants ---
APP_NAME = "Case Log Tool v6"
DB_FILENAME = "caselog_gui_v6.db"
# Define DATA_DIR relative to the script's directory
if getattr(sys, 'frozen', False):
    # Running as a PyInstaller bundle
    SCRIPT_DIR = os.path.dirname(sys.executable)
else:
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "app_data")
LOG_FILENAME = os.path.join(DATA_DIR, "app.log")
LOGO_FILENAME = os.path.join(DATA_DIR, "logo.png")
MARKER_ICON_FILENAME = os.path.join(DATA_DIR, "marker_icon.png") # New constant for custom marker icon

DEFAULT_PASSWORD = "admin" # Default password

# Default Marker Icon (loaded on init)
DEFAULT_MARKER_ICON = None # Global variable for the map view


# US State Abbreviations for State of Offense dropdown
US_STATE_ABBREVIATIONS = [
    "", "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA",
    "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY",
    "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX",
    "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC", "PR", "VI", "AS", "GU", "MP", "UM", "US"
]

# Ensure data directory exists
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

# --- Logging Setup ---
# Check if log file exists and is accessible, if not, log to console
try:
    # Attempt to open for writing to check access
    with open(LOG_FILENAME, 'a') as f:
        pass
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler(LOG_FILENAME, mode='a'),
                                  logging.StreamHandler(sys.stdout)]) # Also log to console
except Exception as e:
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.StreamHandler(sys.stdout)]) # Log only to console if file fails
    logging.error(f"Could not write to log file {LOG_FILENAME}: {e}. Logging only to console.")


logging.info(f"Application '{APP_NAME}' started.")
logging.info(f"Database: {DB_FILENAME}, Data Directory: {DATA_DIR}")


# --- Database Functions ---

def init_db():
    """Initializes the SQLite database and creates the case_log table if it doesn't exist."""
    conn = None # Initialize conn to None
    try:
        conn = sqlite3.connect(DB_FILENAME)
        cursor = conn.cursor()

        # Create case_log table with an auto-incrementing primary key 'id'
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS case_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_number TEXT UNIQUE NOT NULL,
                examiner TEXT,
                investigator TEXT,
                agency TEXT,
                city_of_offense TEXT,
                state_of_offense TEXT,
                start_date TEXT, --YYYY-MM-DD format
                end_date TEXT,   --YYYY-MM-DD format
                volume_size_gb REAL, -- Use REAL for floating point numbers
                offense_type TEXT,
                device_type TEXT,
                model TEXT,
                os TEXT,
                data_recovered TEXT, -- "Yes", "No", or ""
                fpr_complete INTEGER, -- 0 for False, 1 for True
                notes TEXT,
                created_at TEXT -- Store creation timestamp<C2><A0>MM-DD HH:MM:SS
            )
        ''')

        # Create settings table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')

        # Check if password hash exists, if not, set default
        cursor.execute("SELECT value FROM settings WHERE key = 'password_hash'")
        if cursor.fetchone() is None:
            salt = generate_salt()
            hashed_password = hash_password(DEFAULT_PASSWORD, salt)
            cursor.execute("INSERT INTO settings (key, value) VALUES (?, ?)", ('password_hash', hashed_password))
            cursor.execute("INSERT INTO settings (key, value) VALUES (?, ?)", ('salt', salt)) # Store salt separately
            logging.info("Default password hash and salt set in settings.")

        # Create geocache table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS geocache (
                location_key TEXT PRIMARY KEY, -- e.g., "City|State"
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                last_accessed TEXT 
            )
        ''')
        logging.info("Geocache table initialized or already exists.")

        conn.commit()
        logging.info("Database initialized successfully.")

    except sqlite3.Error as e:
        logging.error(f"Database error during initialization: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during database initialization: {e}")
    finally:
        if conn:
            conn.close()

def get_cached_location_db(location_key):
    """Retrieves cached latitude and longitude for a location_key."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILENAME)
        cursor = conn.cursor()
        cursor.execute("SELECT latitude, longitude FROM geocache WHERE location_key = ?", (location_key,))
        row = cursor.fetchone()
        if row:
            # Optionally, update last_accessed timestamp if you want to manage cache eviction later
            # timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            # cursor.execute("UPDATE geocache SET last_accessed = ? WHERE location_key = ?", (timestamp, location_key))
            # conn.commit()
            logging.debug(f"Cache hit for location_key: {location_key}")
            return row[0], row[1]
        logging.debug(f"Cache miss for location_key: {location_key}")
        return None
    except Exception as e:
        logging.error(f"Error retrieving cached location for '{location_key}': {e}")
        return None
    finally:
        if conn:
            conn.close()

def add_cached_location_db(location_key, latitude, longitude):
    """Adds or updates a location in the geocache."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILENAME)
        cursor = conn.cursor()
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute('''
            INSERT OR REPLACE INTO geocache (location_key, latitude, longitude, last_accessed)
            VALUES (?, ?, ?, ?)
        ''', (location_key, latitude, longitude, timestamp))
        conn.commit()
        logging.info(f"Cached/Updated location '{location_key}': {latitude}, {longitude}")
        return True
    except Exception as e:
        logging.error(f"Error caching location '{location_key}': {e}")
        return False
    finally:
        if conn:
            conn.close()

def add_case_db(case_data):
    """Adds a new case to the database."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILENAME)
        cursor = conn.cursor()

        # Ensure the case_number is present and not empty
        case_number = case_data.get("case_number")
        if not case_number or not str(case_number).strip():
            logging.warning("Attempted to add case without a case number.")
            # messagebox.showwarning("Validation Error", "Case Number is required."); # Avoid messagebox in helper
            return False

        # Convert boolean for fpr_complete to integer 0 or 1
        fpr_int = 1 if case_data.get("fpr_complete") else 0
        # Convert boolean for data_recovered to string "Yes" or "No" or ""
        dr_val = case_data.get("data_recovered")
        dr_str = "Yes" if dr_val is True else ("No" if dr_val is False else "") # Convert bool to Yes/No string


        # Get current timestamp for created_at
        created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Use .get() with default None for fields that might be missing in the dictionary
        cursor.execute('''
            INSERT INTO case_log (
                case_number, examiner, investigator, agency, city_of_offense, state_of_offense,
                start_date, end_date, volume_size_gb, offense_type, device_type, model, os,
                data_recovered, fpr_complete, notes, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            str(case_data.get("case_number")).strip(), # Ensure case number is stripped string
            case_data.get("examiner"),
            case_data.get("investigator"),
            case_data.get("agency"),
            case_data.get("city_of_offense"),
            case_data.get("state_of_offense"),
            case_data.get("start_date"),
            case_data.get("end_date"),
            case_data.get("volume_size_gb"),
            case_data.get("offense_type"),
            case_data.get("device_type"),
            case_data.get("model"),
            case_data.get("os"),
            dr_str, # Store "Yes", "No", or ""
            fpr_int, # Store 0 or 1
            case_data.get("notes"),
            created_at
        ))
        conn.commit()
        logging.info(f"Case '{case_number}' added to database.")
        return True
    except sqlite3.IntegrityError:
        logging.warning(f"Case '{case_data.get('case_number', 'N/A')}' already exists.")
        # messagebox.showwarning("Duplicate Entry", f"Case '{case_data.get('case_number', 'N/A')}' already exists in the database."); # Avoid messagebox in helper
        return False
    except Exception as e:
        logging.error(f"Error adding case '{case_data.get('case_number', 'N/A')}' to database: {e}")
        # messagebox.showerror("DB Error", f"Failed to add case: {e}"); # Avoid messagebox in helper
        return False
    finally:
        if conn:
            conn.close()

def get_all_cases_db():
    """Retrieves all cases from the database."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILENAME)
        conn.row_factory = sqlite3.Row  # To access columns by name
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM case_log")
        rows = cursor.fetchall()
        # Convert rows to list of dictionaries
        return [dict(row) for row in rows]
    except Exception as e:
        logging.error(f"Error retrieving all cases from database: {e}")
        return []
    finally:
        if conn:
            conn.close()

def get_case_by_number_db(case_number):
    """Retrieves a single case by its case number."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILENAME)
        conn.row_factory = sqlite3.Row  # To access columns by name
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM case_log WHERE case_number = ?", (str(case_number).strip(),)) # Ensure search is stripped
        row = cursor.fetchone()
        return dict(row) if row else None
    except Exception as e:
        logging.error(f"Error retrieving case by number '{case_number}': {e}")
        return None
    finally:
        if conn:
            conn.close()

def get_case_by_id_db(case_id):
    """Retrieves a single case by its database ID."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILENAME)
        conn.row_factory = sqlite3.Row  # To access columns by name
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM case_log WHERE id = ?", (case_id,))
        row = cursor.fetchone()
        return dict(row) if row else None
    except Exception as e:
        logging.error(f"Error retrieving case by ID '{case_id}': {e}")
        return None
    finally:
        if conn:
            conn.close()


def update_case_db(case_id, case_data):
    """Updates an existing case record in the database."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILENAME)
        cursor = conn.cursor()

        # Construct the SET part of the SQL query dynamically
        # Exclude 'id', 'case_number' (shouldn't be updated directly via edit form submit), and 'created_at'
        fields_to_update = [field for field in case_data.keys() if field not in ['id', 'case_number', 'created_at']]
        set_clause = ', '.join([f'{field} = ?' for field in fields_to_update])

        if not set_clause:
            logging.warning(f"No valid fields to update for case ID {case_id}.")
            return False # Nothing to update

        # Convert boolean for fpr_complete to integer 0 or 1 for database
        if 'fpr_complete' in fields_to_update:
             case_data['fpr_complete'] = 1 if case_data.get('fpr_complete') else 0

        # Convert boolean for data_recovered to string "Yes" or "No" or ""
        if 'data_recovered' in fields_to_update:
             dr_val = case_data.get('data_recovered')
             case_data['data_recovered'] = "Yes" if dr_val is True else ("No" if dr_val is False else "") # Convert bool to Yes/No string


        # Prepare the values tuple, ensuring the order matches the set_clause
        values = tuple(case_data[field] for field in fields_to_update) + (case_id,)

        cursor.execute(f'''
            UPDATE case_log
            SET {set_clause}
            WHERE id = ?
        ''', values)
        conn.commit()
        logging.info(f"Case ID {case_id} updated successfully in DB.")
        return True
    except Exception as e:
        logging.error(f"Failed to update case ID {case_id} in DB: {e}")
        # messagebox.showerror("DB Error", f"Update case failed for ID {case_id}: {e}"); # Avoid messagebox in helper
        return False
    finally:
        if conn:
            conn.close()


def delete_case_db(case_id):
    """Deletes a case record from the database by its ID."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILENAME)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM case_log WHERE id = ?", (case_id,))
        conn.commit()
        logging.info(f"Case ID {case_id} deleted successfully from DB.")
        return True
    except Exception as e:
        logging.error(f"Failed to delete case ID {case_id} from DB: {e}")
        # messagebox.showerror("DB Error", f"Delete case failed for ID {case_id}: {e}"); # Avoid messagebox in helper
        return False
    finally:
        if conn:
            conn.close()


def generate_salt(length=16):
    """Generates a random salt for password hashing."""
    return secrets.token_hex(length)

def hash_password(password, salt):
    """Hashes a password using PBKDF2."""
    # Use a strong KDF like PBKDF2
    # It's recommended to use a higher number of iterations in production
    hashed = hashlib.pbkdf2_hmac('sha256',
                                 password.encode('utf-8'), # Convert password to bytes
                                 salt.encode('utf-8'),     # Convert salt to bytes
                                 100000) # Number of iterations
    return hashed.hex() # Convert hash to hex string for storage

def verify_password(password):
    """Verifies a password against the stored hash and salt."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILENAME)
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM settings WHERE key = 'password_hash'")
        stored_hash_row = cursor.fetchone()
        cursor.execute("SELECT value FROM settings WHERE key = 'salt'")
        stored_salt_row = cursor.fetchone()

        if stored_hash_row and stored_salt_row:
            stored_hash = stored_hash_row[0]
            stored_salt = stored_salt_row[0]
            # Hash the provided password with the stored salt
            hashed_provided_password = hash_password(password, stored_salt)
            return hashed_provided_password == stored_hash
        else:
            logging.warning("Password hash or salt not found in settings DB.")
            return False # Should not happen if init_db runs correctly
    except Exception as e:
        logging.error(f"Error verifying password: {e}")
        return False
    finally:
        if conn:
            conn.close()

def update_password_db(new_password):
    """Updates the stored password hash and salt in the database."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILENAME)
        cursor = conn.cursor()
        salt = generate_salt()
        hashed_password = hash_password(new_password, salt)
        cursor.execute("REPLACE INTO settings (key, value) VALUES (?, ?)", ('password_hash', hashed_password))
        cursor.execute("REPLACE INTO settings (key, value) VALUES (?, ?)", ('salt', salt))
        conn.commit()
        logging.info("Password updated successfully in DB.")
        return True
    except Exception as e:
        logging.error(f"Error updating password in DB: {e}")
        return False
    finally:
        if conn:
            conn.close()


# --- Helper Functions ---

def format_date_str_for_display(date_str):
    """Formats aYYYY-MM-DD date string to MM-DD-YYYY for display."""
    if not date_str:
        return ""
    try:
        # Attempt to parse bothYYYY-MM-DD andYYYY-MM-DD HH:MM:SS formats
        try:
            date_obj = datetime.strptime(str(date_str), '%Y-%m-%d').date()
        except ValueError: # Try with time if initial parse fails
             date_obj = datetime.strptime(str(date_str), '%Y-%m-%d %H:%M:%S').date()

        return date_obj.strftime('%m-%d-%Y')
    except Exception:
        logging.warning(f"Could not parse date string '{date_str}' for display formatting.")
        return str(date_str) # Return original if parsing fails


def format_bool_int(value):
    """Formats a 0 or 1 integer to 'Yes', 'No', or '' for display."""
    if value == 1:
        return "Yes"
    elif value == 0:
        return "No"
    else:
        return "" # Handle None or other values


# --- Main Application Class ---

class CaseLogApp:
    def __init__(self, root):
        self.root = root
        self.root.title(APP_NAME)
        self.root.geometry("1250x850")
        self.style = ttk.Style()
        self.style.theme_use('clam')

        # Attributes for entry widgets
        self.entries = {} # Dictionary to hold Tkinter variables/widgets for form fields
        self.editing_case_id = None # Variable to track if we are currently editing a case (None or case_id)
        self.submit_button = None # Reference to the submit button for text changes
        self.field_frame_container = None # Reference to the frame holding input fields

        # Attributes for logo image
        self.logo_path = tk.StringVar(value=LOGO_FILENAME) # Track the path, though we primarily use the loaded image
        self.logo_image_tk = None # Image for display in the Entry tab (scaled)
        self.logo_image_tk_preview = None # Separate image for the settings preview (thumbnail)
        self.entry_logo_label = None # Attribute to store the logo label in the Entry tab (needed to update its image)
        self.logo_preview_canvas = None # Reference to the settings logo preview canvas

        # Attributes for marker icon images
        self.marker_icon_tk_map = None # Image for map markers (20x20)
        self.marker_icon_tk_preview = None # Image for settings preview (e.g., 50x50)
        self.marker_icon_preview_canvas = None # Reference to the settings preview canvas

        self.load_logo_image() # Load the logo upon app initialization
        self.load_marker_icon_image() # Load the marker icon upon app initialization


        # Attributes for Map View
        self.map_widget = None
        self.map_status_label = None
        # Geopy geolocator instance - only create one per thread. Not needed in main thread.
        # self.geolocator = Nominatim(user_agent=APP_NAME)
        self.map_markers = {} # Dictionary to hold mapview markers with location (city, state) as key
        self._grouped_cases_by_location = {} # Store cases grouped by location for info bubbles


        # Attributes for View Data Treeview
        self.tree = None
        self.tree_columns_config = {} # Dictionary to store treeview column configuration
        self.treeview_sort_column = None # To keep track of the currently sorted column
        self.treeview_sort_reverse = False # To keep track of the sort order

        # Attributes for Graph Tab
        self.fig = None # Matplotlib figure
        self.ax = None # Matplotlib axes
        self.canvas_agg = None # FigureCanvasTkAgg

        # Attributes for Status Bar
        self.status_label = None
        self.status_animation_id = None
        self.status_text = ""


        # Attributes for threading and queue for map loading
        self.geocoding_queue = queue.Queue()
        self.geocoding_thread = None
        self.processing_queue = False # Flag to indicate if we are currently checking the queue
        self.geolocated_count = 0 # Initialize count for geolocated markers (locations)
        self.skipped_count = 0 # Initialize count for skipped locations
        self._geocoding_after_id = None # ID for the scheduled _process_geocoding_results after call


        self.create_widgets() # Create all the main UI widgets

        # Status Bar creation (Moved here to ensure self.status_label exists before status updates)
        self.status_label = ttk.Label(self.root, text="Initializing...", anchor='w')
        self.status_label.pack(side='bottom', fill='x', padx=10, pady=(0, 5))
        self.update_status("Initializing application...")


        # Perform initial data loading and UI refresh
        self.refresh_data_view() # Populate the treeview
        self.load_map_markers() # This now starts the threaded geocoding
        self.populate_graph_filters() # Populate filters for the graph
        self.update_graph() # Display initial graph

        # Initial status is set by the map loading process or defaults below if map loading is skipped
        # The _finalize_map_loading will set the final status
        # Ensure status is cleared if thread finishes quickly
        if not self.geocoding_thread or not self.geocoding_thread.is_alive():
             self.update_status("Ready")

        # Set the window closing protocol to call the cleanup function
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)


    def create_widgets(self):
        """Creates the main notebook tabs and calls methods to populate them."""
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(pady=10, padx=10, fill='both', expand=True)

        # Create frames for each tab
        self.entry_frame = ttk.Frame(self.notebook, padding="10")
        self.view_frame = ttk.Frame(self.notebook, padding="10")
        self.map_frame = ttk.Frame(self.notebook, padding="10")
        self.graph_frame = ttk.Frame(self.notebook, padding="10")
        self.settings_frame = ttk.Frame(self.notebook, padding="10")

        # Add frames as tabs to the notebook
        self.notebook.add(self.entry_frame, text='New Case Entry')
        self.notebook.add(self.view_frame, text='View Data')
        self.notebook.add(self.map_frame, text='Map View')
        self.notebook.add(self.graph_frame, text='Graphs')
        self.notebook.add(self.settings_frame, text='Settings')

        # Create widgets for each tab
        self.create_entry_widgets()
        self.create_view_widgets()
        self.create_map_widgets()
        self.create_graph_widgets()
        self.create_settings_widgets()

        # Status Bar creation is now moved to __init__


    def create_entry_widgets(self):
        """Creates the widgets for the New Case Entry tab."""
        # Create a main frame that will hold all content for the entry tab
        entry_content_frame = ttk.Frame(self.entry_frame)
        entry_content_frame.pack(fill='both', expand=True)

        # Create a Canvas and Scrollbar for the scrollable area
        canvas = tk.Canvas(entry_content_frame)
        scrollbar = ttk.Scrollbar(entry_content_frame, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=scrollbar.set)

        # Pack the scrollbar and canvas
        scrollbar.pack(side="right", fill="y")
        canvas.pack(side="left", fill="both", expand=True)

        # Create the frame that will be inside the canvas and hold all your scrollable widgets
        scrollable_frame = ttk.Frame(canvas)

        # Put the scrollable_frame inside the canvas window
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")

        # Configure the canvas scroll region to be the size of the scrollable_frame
        scrollable_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion = canvas.bbox("all")))

        # --- Now, place all your subsequent widgets inside scrollable_frame ---


        # Top section: Title and Logo
        top_section_frame = ttk.Frame(scrollable_frame) # Parent is scrollable_frame
        top_section_frame.pack(fill='x', pady=10, padx=10) # Pack within scrollable_frame

        title_label = ttk.Label(top_section_frame, text="New Case Entry", font=("Arial", 16, "bold"))
        title_label.pack(side='left', anchor='nw')

        # Logo label in the top section (initial text, image set in load_logo_image)
        self.entry_logo_label = ttk.Label(top_section_frame, text="No Logo")
        self.entry_logo_label.pack(side='right', anchor='ne', pady=5) # Pack within top_section_frame
        # Initial logo update is now called in __init__ after load_logo_image


        self.entries = {} # Dictionary to hold Tkinter variables/widgets for form fields
        # Frame to hold the grid of input fields
        self.field_frame_container = ttk.Frame(scrollable_frame) # Parent is scrollable_frame, store reference
        # Pack the field_frame_container below the top_section_frame within the scrollable_frame
        self.field_frame_container.pack(fill='x', expand=False, anchor='nw', pady=(10,0), padx=10) # Add padding

        # Define the fields to be created: (Label Text, Dictionary Key, Widget Type, Options)
        # Widget Type: "entry", "combo", "check", "date", "text"
        fields_definition = [
            ("Examiner", "examiner", "entry"),
            ("Investigator", "investigator", "entry"),
            ("Agency", "agency", "entry"),
            ("Cyber Case #", "case_number", "entry"),
            ("Volume Size (GB)", "volume_size_gb", "entry"),
            ("Type of Offense", "offense_type", "entry"),
            ("City of Offense", "city_of_offense", "entry"),
            ("State of Offense", "state_of_offense", "combo", US_STATE_ABBREVIATIONS), # Added State here
            ("Device Type", "device_type", "combo", ["", "iOS", "Android", "ChromeOS", "Windows", "SD", "HDD", "SDD", "USB", "SW Return", "Zip file", "drone", "other"]),
            ("Model", "model", "entry"),
            ("OS", "os", "entry"),
            ("Data Recovered", "data_recovered", "check"), # Changed to 'check'
            ("FPR Complete", "fpr_complete", "check") # This was changed to a checkbox
        ]

        current_row = 0 # Initialize row counter for grid layout
        for i, (label_text, key, field_type, *options) in enumerate(fields_definition):
            row, col = divmod(i, 2) # Arrange fields in two columns
            current_row = row # Keep track of the current row used by the loop

            cell_frame = ttk.Frame(self.field_frame_container, padding=(0,0,10,5)) # Parent is field_frame_container
            cell_frame.grid(row=row, column=col, sticky='ew', padx=5, pady=2)
            self.field_frame_container.grid_columnconfigure(col, weight=1) # Make columns expandable

            label = ttk.Label(cell_frame, text=label_text)
            label.pack(side='top', anchor='w')

            if field_type == "entry":
                entry = ttk.Entry(cell_frame, width=40)
                entry.pack(side='top', fill='x', expand=True)
                self.entries[key] = entry
            elif field_type == "combo":
                var = tk.StringVar()
                combo_values = options[0] if options and options[0] else [] # Get the list of choices
                combo = ttk.Combobox(cell_frame, textvariable=var, values=combo_values, state="readonly", width=38)
                combo.pack(side='top', fill='x', expand=True)
                if key == "state_of_offense": # Set default for State of Offense
                     if "MS" in combo_values:
                          var.set("MS")
                     elif combo_values:
                          var.set(combo_values[0]) # Fallback to first item if MS not in list
                elif combo_values: # Set default to the first item for other combos
                    var.set(combo_values[0])

                self.entries[key] = var
            elif field_type == "check":
                var = tk.BooleanVar()
                chk_frame = ttk.Frame(cell_frame) # Frame to hold the checkbox and potentially a label
                chk = ttk.Checkbutton(chk_frame, variable=var)
                # No label needed next to checkbox as the main label is above
                chk.pack(side='left', anchor='w')
                self.entries[key] = var
                chk_frame.pack(side='top', anchor='w', fill='x')


        # --- Notes field ---
        # This block should come immediately after the main fields_definition loop
        notes_row = current_row + 1 # Calculate the row for the Notes field based on the last row from the loop
        notes_frame = ttk.LabelFrame(self.field_frame_container, text="Notes", padding="5") # Parent is field_frame_container
        notes_frame.grid(row=notes_row, column=0, columnspan=2, sticky='ewns', padx=5, pady=(10,5))
        self.field_frame_container.grid_rowconfigure(notes_row, weight=1) # Allow notes field to expand vertically

        txt_notes = tk.Text(notes_frame, height=6, width=40, wrap='word')
        txt_notes_scroll = ttk.Scrollbar(notes_frame, orient='vertical', command=txt_notes.yview)
        txt_notes['yscrollcommand'] = txt_notes_scroll.set

        txt_notes_scroll.pack(side='right', fill='y')
        txt_notes.pack(side='left', fill='both', expand=True)

        self.entries['notes'] = txt_notes # Store the Text widget reference

        # --- DateEntry Fields ---
        # This block must come AFTER the Notes field block (where notes_row is defined)
        date_row = notes_row + 1 # Calculate the row for DateEntry fields based on the Notes field's row

        date_field_info = [("Start Date (MM-DD-YYYY)", "start_date"), ("End Date (MM-DD-YYYY)", "end_date")]
        for i, (label_text, key) in enumerate(date_field_info):
            col = i # Dates will be side-by-side (column 0 and 1)
            cell_frame = ttk.Frame(self.field_frame_container, padding=(0,0,10,5)) # Parent is field_frame_container
            cell_frame.grid(row=date_row, column=col, sticky='ew', padx=5, pady=2) # Use date_row

            label = ttk.Label(cell_frame, text=label_text)
            label.pack(side='top', anchor='w')

            # Use tkcalendar.DateEntry
            date_entry = DateEntry(cell_frame, width=38, background='darkblue', foreground='white', borderwidth=2, date_pattern='mm-dd-yyyy', firstweekday='sunday', showweeknumbers=False, state="readonly")
            date_entry.pack(side='top', fill='x', expand=True)
            date_entry.set_date(None) # Start with no date selected
            self.entries[key] = date_entry # Store the DateEntry widget reference


        # --- Submit and Cancel Buttons ---
        # This frame should be placed after the date fields. Determine the row after date fields.
        # Assuming DateEntry fields are on one row (date_row), the buttons go on the next row.
        submit_button_row = date_row + 1

        submit_button_frame = ttk.Frame(scrollable_frame) # Parent is scrollable_frame
        submit_button_frame.pack(fill='x', pady=(15, 10), anchor='w', padx=10) # Pack within scrollable_frame

        # Submit button (store reference)
        self.submit_button = ttk.Button(submit_button_frame, text="Submit Case", command=self.submit_case, style="Accent.TButton")
        self.submit_button.pack(side='left') # Pack left

        # Add a Cancel Edit/Clear Form button
        cancel_button = ttk.Button(submit_button_frame, text="Clear Form", command=self.clear_entry_form)
        cancel_button.pack(side='left', padx=(5,0)) # Pack next to submit button

        # Configure Accent button style (defined here as used in this tab)
        self.style.configure("Accent.TButton", font=("-weight", "bold"))


    def create_view_widgets(self):
        """Creates the widgets for the View Data tab (Treeview, buttons)."""
        container = ttk.Frame(self.view_frame)
        container.pack(fill='both', expand=True)

        # Button frame for Refresh, Export, Edit, Delete
        button_frame = ttk.Frame(container)
        button_frame.pack(fill='x', pady=(0, 10), anchor='w', padx=5) # Anchor West, add padx

        refresh_button = ttk.Button(button_frame, text="Refresh Data", command=self.refresh_data_view)
        refresh_button.pack(side='left', padx=(0, 5))

        pdf_button = ttk.Button(button_frame, text="Export All as PDF", command=self.export_pdf_report)
        pdf_button.pack(side='left', padx=(0,5))

        xlsx_button = ttk.Button(button_frame, text="Export All as XLSX", command=self.export_xlsx_report)
        xlsx_button.pack(side='left', padx=(0,5))

        # Add Edit Selected button
        edit_button = ttk.Button(button_frame, text="Edit Selected", command=self.edit_selected_case)
        edit_button.pack(side='left', padx=(0,5)) # Pack left, add some padding

        # Add a Delete Selected button
        delete_button = ttk.Button(button_frame, text="Delete Selected", command=self.delete_selected_cases, style="Danger.TButton") # Use Danger style for delete
        delete_button.pack(side='left') # Pack left

        # Frame to hold the Treeview and its scrollbars
        tree_frame = ttk.Frame(container)
        tree_frame.pack(fill='both', expand=True, padx=5, pady=5) # Add padding

        self.tree = ttk.Treeview(tree_frame, show='headings')

        # Store the database column names along with display text and other config
        # Ensure 'id' is included but marked as not visible
        self.tree_columns_config = {
            "id": {"text": "ID", "width": 0, "visible": False}, # Keep ID for deletion/editing but hide
            "case_number": {"text": "Case #", "width": 100},
            "examiner": {"text": "Examiner", "width": 100},
            "investigator": {"text": "Investigator", "width": 100},
            "agency": {"text": "Agency", "width": 100},
            "city_of_offense": {"text": "City", "width": 100},
            "state_of_offense": {"text": "State", "width": 80},
            "start_date": {"text": "Start (MM-DD-YYYY)", "width": 100, "type": "date"},
            "end_date": {"text": "End (MM-DD-YYYY)", "width": 100, "type": "date"},
            "volume_size_gb": {"text": "Vol (GB)", "width": 60, "type": "numeric"},
            "offense_type": {"text": "Offense", "width": 120},
            "device_type": {"text": "Device", "width": 100},
            "model": {"text": "Model", "width": 100},
            "os": {"text": "OS", "width": 80},
            "data_recovered": {"text": "Recovered?", "width": 70}, # Keep text, will display Yes/No
            "fpr_complete": {"text": "FPR?", "width": 50, "type": "boolean"},
            "created_at": {"text": "Created (MM-DD-YYYY)", "width": 100, "type": "date"},
            "notes": {"text": "Notes", "width": 200}
        }

        # Use all keys from config as internal treeview columns
        self.tree["columns"] = list(self.tree_columns_config.keys())
        # Use only visible columns for Treeview display columns
        visible_columns = [key for key, config in self.tree_columns_config.items() if config.get("visible", True)]
        self.tree.configure(displaycolumns=visible_columns)


        for col_key, config in self.tree_columns_config.items():
            # Configure headings only for displayed columns
            if col_key in visible_columns:
                 self.tree.heading(col_key, text=config["text"], command=lambda c=col_key: self.sort_treeview_column(c))
            self.tree.column(col_key, anchor='w', width=config["width"], stretch=tk.NO)
            if not config.get("visible", True):
                 self.tree.column(col_key, width=0, stretch=tk.NO) # Hide the column


        # Scrollbars for the Treeview
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        vsb.pack(side='right', fill='y')
        hsb.pack(side='bottom', fill='x')
        self.tree.pack(side='left', fill='both', expand=True)

        # Bind right-click for context menu (optional, but good for editing/deleting)
        # self.tree.bind("<Button-3>", self.on_treeview_right_click) # Implement on_treeview_right_click method

        # Configure Danger button style (defined here as used in this tab)
        self.style.configure("Danger.TButton", foreground="red", font=("-weight", "bold"))


    def create_map_widgets(self):
        """Creates the widgets for the Map View tab."""
        container = ttk.Frame(self.map_frame)
        container.pack(fill='both', expand=True)

        # Map widget
        self.map_widget = tkintermapview.TkinterMapView(container, width=800, height=600, corner_radius=0)
        self.map_widget.pack(fill='both', expand=True)

        # Set initial position and zoom to Mississippi
        # Approximate center of Mississippi: 32.7 N, 89.5 W
        self.map_widget.set_position(32.7, -89.5)
        self.map_widget.set_zoom(7) # Zoom level 7 might show most of the state

        # Status label for map loading
        self.map_status_label = ttk.Label(container, text="Map status: Not Loaded", anchor='w')
        self.map_status_label.pack(side='bottom', fill='x', pady=(5, 0))

    # Add this method inside your CaseLogApp class
    def on_marker_click(self, marker):
        """
        Handles the event when a map marker is clicked.
        Displays the stored information in a messagebox.
        """
        if marker.data: # The info_text will be stored in marker.data
            info_to_display = str(marker.data) # Ensure it's a string

            # Try to create a more relevant title for the messagebox
            title = "Marker Information"
            try:
                # Attempt to extract city from the first line of info_to_display for the title
                first_line = info_to_display.split('\n', 1)[0]
                if "City of Offense:" in first_line:
                    title = first_line
            except Exception:
                pass # Keep default title if parsing fails

            messagebox.showinfo(title, info_to_display)
            logging.debug(f"Marker clicked at ({marker.position[0]:.2f}, {marker.position[1]:.2f}). Displaying data.")
        else:
            # This case should ideally not happen if data is always set
            messagebox.showinfo("Marker Information", "No specific information available for this marker.")
            logging.debug(f"Marker clicked at ({marker.position[0]:.2f}, {marker.position[1]:.2f}), but no data was found.")

    def create_graph_widgets(self):
        """Creates the widgets for the Graphs tab."""
        container = ttk.Frame(self.graph_frame)
        container.pack(fill='both', expand=True)

        # Controls frame (filters, buttons)
        controls_frame = ttk.Frame(container)
        controls_frame.pack(fill='x', pady=5, padx=10)

        ttk.Label(controls_frame, text="Graph Type:").pack(side='left', padx=(0, 5))
        self.graph_type_var = tk.StringVar(value="Offense Type")
        self.graph_type_combo = ttk.Combobox(controls_frame, textvariable=self.graph_type_var,
                                             values=["Offense Type", "Device Type", "OS", "Agency", "State of Offense"],
                                             state="readonly")
        self.graph_type_combo.pack(side='left', padx=(0, 10))
        self.graph_type_combo.bind("<<ComboboxSelected>>", lambda e: self.update_graph()) # Update graph on selection

        ttk.Label(controls_frame, text="Filter by Year:").pack(side='left', padx=(0, 5))
        self.graph_year_var = tk.StringVar(value="All")
        self.graph_year_combo = ttk.Combobox(controls_frame, textvariable=self.graph_year_var,
                                             values=["All"], state="readonly", width=8) # Years populated later
        self.graph_year_combo.pack(side='left', padx=(0, 10))
        self.graph_year_combo.bind("<<ComboboxSelected>>", lambda e: self.update_graph()) # Update graph on selection


        # Frame for the matplotlib graph
        graph_frame = ttk.Frame(container)
        graph_frame.pack(fill='both', expand=True, padx=10, pady=10)

        # Initialize matplotlib figure and axes
        self.fig, self.ax = plt.subplots(figsize=(10, 6)) # Adjust figure size as needed

        # Create a canvas to display the figure in tkinter
        self.canvas_agg = FigureCanvasTkAgg(self.fig, master=graph_frame)
        self.canvas_agg.get_tk_widget().pack(fill='both', expand=True)


    def create_settings_widgets(self):
        """Creates the widgets for the Settings tab."""
        settings_content_frame = ttk.Frame(self.settings_frame)
        settings_content_frame.pack(fill='both', expand=True, anchor='nw')

        # --- Report Header Logo Section ---
        logo_section_frame = ttk.Frame(settings_content_frame)
        logo_section_frame.pack(fill='x', pady=10, anchor='w', padx=10)

        ttk.Label(logo_section_frame, text="Header Logo:", font=("-weight", "bold")).pack(anchor='w', pady=(0, 5))
        ttk.Label(logo_section_frame, text=f"Select image (png, jpg, jpeg, gif).\nSaved as logo.png in:\n{DATA_DIR}").pack(anchor='w', pady=(0, 10))

        select_logo_button_frame = ttk.Frame(logo_section_frame)
        select_logo_button_frame.pack(fill='x', pady=5, anchor='w')

        select_button = ttk.Button(select_logo_button_frame, text="Select Logo File...", command=self.select_logo)
        select_button.pack(side='left')

        # Canvas for logo preview
        self.logo_preview_canvas = tk.Canvas(logo_section_frame, width=200, height=100, bg="lightgrey", relief="sunken")
        self.logo_preview_canvas.pack(pady=10, anchor='w')
        # Initial preview update is now called in __init__ after load_logo_image


        # --- Map Marker Icon Section ---
        marker_icon_section_frame = ttk.Frame(settings_content_frame)
        marker_icon_section_frame.pack(fill='x', pady=10, anchor='w', padx=10) # Add padx

        ttk.Label(marker_icon_section_frame, text="Map Marker Icon:", font=("-weight", "bold")).pack(anchor='w', pady=(0, 5))
        ttk.Label(marker_icon_section_frame, text=f"Select image (png, jpg, jpeg, gif) for map markers.\nSaved as marker_icon.png in:\n{DATA_DIR}\n(If no custom icon, default 'marker_pin.png' or library default is used.)").pack(anchor='w', pady=(0, 10)) # Added info about fallback

        select_marker_icon_button_frame = ttk.Frame(marker_icon_section_frame)
        select_marker_icon_button_frame.pack(fill='x', pady=5, anchor='w')

        select_marker_button = ttk.Button(select_marker_icon_button_frame, text="Select Marker Icon File...", command=self.select_marker_icon)
        select_marker_button.pack(side='left')

        # Canvas for marker icon preview (smaller size)
        self.marker_icon_preview_canvas = tk.Canvas(marker_icon_section_frame, width=50, height=50, bg="lightgrey", relief="sunken") # Smaller canvas
        self.marker_icon_preview_canvas.pack(pady=10, anchor='w')
        # Initial preview update is now called in __init__ after load_marker_icon_image


        # --- Action Buttons Frame (packed left) ---
        buttons_area_frame = ttk.Frame(settings_content_frame)
        buttons_area_frame.pack(fill='x', pady=10, anchor='w', padx=10)

        import_button = ttk.Button(buttons_area_frame, text="Import Cases from XLSX", command=self.import_cases_from_xlsx)
        import_button.pack(side='left', pady=(5,0), padx=(0,5))

        log_button = ttk.Button(buttons_area_frame, text="View Application Log", command=self.show_application_log)
        log_button.pack(side='left', pady=(5,0), padx=(0,5))

        change_pw_button = ttk.Button(buttons_area_frame, text="Change Password", command=self.change_password_prompt)
        change_pw_button.pack(side='left', pady=(5,0), padx=(0,5))

        clear_data_button = ttk.Button(buttons_area_frame, text="Clear Application Data", command=self.clear_application_data_prompt, style="Danger.TButton")
        clear_data_button.pack(side='left', pady=(5,0), padx=(0,5))


        # Display default password and warning
        password_warning_label = ttk.Label(settings_content_frame,
                                           text=f"Default Password: {DEFAULT_PASSWORD}\n(It is highly recommended to change the default password for security.)",
                                           foreground="black")
        password_warning_label.pack(pady=(15, 0), padx=10, anchor='w')

        # Note about Geocoding limits
        geocoding_note_label = ttk.Label(settings_content_frame,
                                           text="Note: Map geocoding uses Nominatim, which has usage policies.\nPlease use responsibly.",
                                           foreground="gray")
        geocoding_note_label.pack(pady=(5, 0), padx=10, anchor='w')


    # --- Data Handling and UI Refresh ---

    def refresh_data_view(self):
        """Clears and re-populates the Treeview with data from the database."""
        self.update_status("Refreshing data...")
        self.root.update_idletasks() # Update status bar immediately
        logging.info("Starting data refresh for Treeview.")

        # Clear existing items in the treeview
        logging.debug("Clearing existing treeview items.")
        try:
            for item in self.tree.get_children():
                self.tree.delete(item)
            logging.debug("Finished clearing existing treeview items.")
        except Exception as e:
            logging.error(f"Error clearing treeview items: {e}")
            # Continue execution, but log the error


        logging.debug("Fetching all cases from database.")
        try:
            cases = get_all_cases_db()
            logging.debug(f"Fetched {len(cases)} cases from database.")
        except Exception as e:
            logging.error(f"Error fetching cases from database: {e}")
            cases = [] # Continue with empty list if fetch fails
            self.update_status("Error fetching data.")


        # Get the keys in the order they are defined in tree_columns_config, including 'id'
        column_keys_ordered = list(self.tree_columns_config.keys())

        logging.debug("Starting insertion of cases into treeview.")
        try:
            for index, case in enumerate(cases):
                # Prepare the values for the treeview item, ensuring the order matches column_keys_ordered
                # Also format data for display where needed
                values = tuple(
                    format_date_str_for_display(case.get(col_key)) if col_key in ['start_date', 'end_date', 'created_at']
                    else format_bool_int(case.get(col_key)) if col_key == "fpr_complete"
                     # 'data_recovered' is already "Yes"/"No"/"" string from DB, no need for format_bool_int
                    else str(case.get(col_key, '')) if col_key == "volume_size_gb" and case.get(col_key) is not None
                    else case.get(col_key, '') # Use get with default '' for other fields
                    for col_key in column_keys_ordered # Use the ordered keys here
                )
                # The 'iid' is an internal identifier for the treeview item.
                # We use the database 'id' as the 'iid' to easily retrieve it later for deletion/editing.
                # Ensure case.get('id') returns a valid ID (integer)
                case_id = case.get('id')
                if case_id is not None:
                     self.tree.insert("", tk.END, values=values, iid=case_id)
                     # Log progress periodically for very large datasets
                     if len(cases) > 100 and (index + 1) % 100 == 0: # Log every 100 for more visibility
                         logging.debug(f"Inserted {index + 1}/{len(cases)} cases into treeview.")
                else:
                     logging.warning(f"Skipping case at index {index} in Treeview refresh: Missing ID.")


            logging.debug("Finished insertion of cases into treeview.")
        except Exception as e:
            logging.error(f"Error inserting cases into treeview: {e}")
            self.update_status("Error populating view.")


        # Check if a sort was active and re-apply it after refresh
        logging.debug(f"Checking if previous sort was active. Column: {self.treeview_sort_column}")
        try:
            if self.treeview_sort_column:
                 logging.debug(f"Re-applying sort on column: {self.treeview_sort_column}")
                 # Pass the current case data if needed for sorting logic that operates on data directly
                 # Assuming sort_treeview_column only needs the column key and order
                 self.sort_treeview_column(self.treeview_sort_column, initial_sort=True)
                 logging.debug("Finished re-applying sort.")
            else:
                 # If no sort was active, reset headers to plain text
                 logging.debug("No previous sort active. Resetting treeview headers.")
                 for c_key, config in self.tree_columns_config.items():
                     if config.get("visible", True): # Only update text for visible columns
                         self.tree.heading(c_key, text=config["text"])
                 logging.debug("Finished resetting treeview headers.")
        except Exception as e:
            logging.error(f"Error during treeview sorting or header update: {e}")
            # Continue execution, but log the error


        self.update_status(f"Data refreshed. {len(cases)} cases loaded.")
        logging.info("Data refresh for Treeview complete.")


    def submit_case(self):
        """Collects data from the entry form and either adds a new case or updates an existing one."""
        case_data = self.collect_form_data(for_validation=True) # Use helper to collect and strip/format

        # --- Validation ---
        case_number = case_data.get("case_number", "").strip()
        if not case_number:
             messagebox.showwarning("Validation Error", "Case Number is required.")
             logging.warning("Submit failed: Case Number is required.")
             return # Stop if case number is empty

        # Validate and convert volume_size_gb to float or None
        vol_size_str = case_data.get('volume_size_gb', '').strip()
        if vol_size_str:
             try:
                 case_data['volume_size_gb'] = float(vol_size_str)
             except ValueError:
                 messagebox.showwarning("Validation Error", "Volume Size (GB) must be a valid number.")
                 logging.warning(f"Submit failed: Invalid Volume Size (GB) '{vol_size_str}'.")
                 return # Stop if invalid number
        else:
             case_data['volume_size_gb'] = None # Store as None if empty

        # Handle 'data_recovered' - it comes as boolean from the checkbox now
        # Convert boolean to "Yes", "No", or "" string for database storage
        dr_val = case_data.get('data_recovered') # This is True/False
        case_data['data_recovered'] = "Yes" if dr_val is True else ("No" if dr_val is False else "") # Convert bool to Yes/No string

        # Ensure fpr_complete is handled correctly (already was BooleanVar)
        # submit_case handles this conversion to 0/1 for DB before insertion/update


        # --- Insert or Update based on self.editing_case_id ---
        if self.editing_case_id is not None:
            # We are editing an existing case
            case_id_to_update = self.editing_case_id
            logging.info(f"Attempting to update case ID: {case_id_to_update}")

            # Pass the collected case_data dictionary directly to update_case_db
            # update_case_db handles converting boolean fpr_complete to 0/1 for update
            if update_case_db(case_id_to_update, case_data):
                messagebox.showinfo("Success", f"Case ID {case_id_to_update} updated successfully.")
                logging.info(f"Case ID {case_id_to_update} updated.")
                self.clear_entry_form() # Clear form and reset editing state
                self.refresh_data_view() # Refresh the view to show changes
                # Reload map markers and graphs as data might affect them
                if hasattr(self, 'map_widget'):
                     self.load_map_markers() # This will start a new threaded load
                self.populate_graph_filters() # This also calls update_graph
                self.update_status(f"Case ID {case_id_to_update} updated.")

            else:
                # Error message shown by update_case_db logging
                messagebox.showerror("Database Error", f"Failed to update case ID {case_id_to_update}. See log for details.")
                self.update_status(f"Failed to update case ID {case_id_to_update}.")


        else:
            # We are adding a new case
            logging.info(f"Attempting to submit new case: {case_number}")
            # Pass the collected case_data dictionary directly to add_case_db
            # add_case_db handles the bool to int conversion for insert
            if add_case_db(case_data): # add_case_db returns True/False
                messagebox.showinfo("Success", "Case submitted successfully.")
                logging.info(f"New case '{case_number}' submitted.")
                self.clear_entry_form() # Clear form after successful submission
                self.refresh_data_view() # Refresh the view to show the new case
                # Reload map markers and graphs for the new data
                if hasattr(self, 'map_widget'):
                     self.load_map_markers() # This will start a new threaded load
                self.populate_graph_filters() # This also calls update_graph
                self.update_status(f"New case '{case_number}' submitted.")

            else:
                # Error message shown by add_case_db logging (e.g., duplicate if somehow missed get_case_by_number_db)
                messagebox.showerror("Database Error", f"Failed to submit case '{case_number}'. It may already exist. See log for details.")
                self.update_status(f"Failed to submit case '{case_number}'.")

        # No matter if insert or update, refresh related parts of the UI
        # Already done within the if/else blocks above


    def collect_form_data(self, for_validation=True):
        """Collects data from the entry form widgets into a dictionary.
           Handles different widget types.
           Use for_validation=False to collect raw values without stripping."""
        case_data = {}
        for key, widget in self.entries.items():
            if isinstance(widget, ttk.Entry):
                value = widget.get().strip() if for_validation else widget.get()
                case_data[key] = value
            elif isinstance(widget, tk.StringVar): # Combobox StringVar
                value = widget.get().strip() if for_validation else widget.get()
                case_data[key] = value
            elif isinstance(widget, tk.BooleanVar): # Checkbutton BooleanVar
                case_data[key] = widget.get() # This returns True/False directly
            elif isinstance(widget, tk.Text): # Text widget for Notes
                # Get text from 1.0 to end-1c (to exclude the trailing newline)
                value = widget.get("1.0", "end-1c").strip() if for_validation else widget.get("1.0", "end-1c")
                case_data[key] = value
            elif isinstance(widget, DateEntry): # DateEntry widget
                date_obj = widget.get_date()
                # Convert date object toYYYY-MM-DD string or None for DB/validation
                case_data[key] = date_obj.strftime('%Y-%m-%d') if date_obj else None
            # Add handling for other widget types if any exist
            # else:
            #     logging.warning(f"Unknown widget type for key '{key}' during data collection: {type(widget)}")

        return case_data


    def clear_entry_form(self):
        """Clears all input fields in the New Case Entry form and resets the editing state."""
        self.editing_case_id = None # Reset the editing state
        # Reset the submit button text and style
        if self.submit_button: # Check if button exists before configuring
            self.submit_button.config(text="Submit Case", style="Accent.TButton")
        # Reset the tab title
        if hasattr(self, 'notebook') and hasattr(self, 'entry_frame'): # Check if notebook and frame exist
            self.notebook.tab(self.entry_frame, text="New Case Entry")

        # Clear the contents of each widget
        for key, widget in self.entries.items():
            if isinstance(widget, ttk.Entry):
                widget.delete(0, tk.END)
            elif isinstance(widget, tk.StringVar): # Combobox StringVar
                # Set combobox to the first value (usually empty string)
                 combo_widget = None
                 # Find the corresponding combobox widget to get its values
                 # We need to iterate through children of field_frame_container to find the actual Combobox widget
                 if hasattr(self, 'field_frame_container') and self.field_frame_container:
                      for child in self.field_frame_container.winfo_children(): # Iterate through cell frames
                          for grandchild in child.winfo_children(): # Iterate through label and widget in cell frame
                              if isinstance(grandchild, ttk.Combobox):
                                   # Check if the textvariable associated with the combobox matches the current entry key
                                   # Use .cget('textvariable') which returns the internal name or string
                                   # Check if the name of the StringVar matches the key
                                   if isinstance(grandchild.cget('textvariable'), str) and grandchild.cget('textvariable') == key:
                                        combo_widget = grandchild
                                        break # Found the combobox
                                   # Also check if the StringVar object itself is the textvariable
                                   if isinstance(widget, tk.StringVar) and grandchild.cget('textvariable') is widget:
                                        combo_widget = grandchild
                                        break # Found the combobox

                          if combo_widget: break # Stop outer loop if found

                 if combo_widget:
                      current_values = combo_widget.cget('values')
                      if key == "state_of_offense" and "MS" in current_values:
                           widget.set("MS") # Set default to MS for State of Offense
                      elif current_values:
                           widget.set(current_values[0]) # Set to the first option for other combos
                      else: widget.set('') # Set to empty string if no options
                 elif isinstance(widget.get(), str): # If it's just a StringVar not linked to a Combobox (less common in this app structure)
                     widget.set('')


            elif isinstance(widget, tk.BooleanVar): # Checkbutton BooleanVar
                widget.set(False) # Default checkbox to unchecked
            elif isinstance(widget, tk.Text): # Text widget for Notes
                widget.delete('1.0', tk.END) # Delete all text from start to end

            # Handle DateEntry widgets
            elif isinstance(widget, DateEntry):
                 widget.set_date(None) # Set to no date selected

            # Add handling for other widget types if any exist


    def edit_selected_case(self):
        """Retrieves the selected case data and populates the entry form for editing."""
        selected_items = self.tree.selection()
        if not selected_items:
            messagebox.showwarning("No Selection", "Please select a case to edit.")
            logging.warning("Edit failed: No case selected.")
            return
        if len(selected_items) > 1:
            messagebox.showwarning("Multiple Selection", "Please select only one case to edit.")
            logging.warning("Edit failed: Multiple cases selected.")
            return

        selected_item = selected_items[0]
        try:
            # Get the database ID from the treeview item's iid
            # Ensure case_id is treated as integer for DB operation
            case_id = int(self.tree.item(selected_item, 'iid'))
            logging.info(f"Attempting to retrieve case for editing with ID: {case_id}")

            case_data = get_case_by_id_db(case_id)

            if case_data:
                self.update_status(f"Editing Case ID: {case_id}...")
                self.editing_case_id = case_id # Set the instance variable for the case being edited
                self.populate_entry_form(case_data) # Populate the form with retrieved data
                self.notebook.select(self.entry_frame) # Switch to the Entry tab
                # Change button text and tab title to indicate editing mode
                if self.submit_button:
                     self.submit_button.config(text="Update Case", style="Accent.TButton")
                if hasattr(self, 'notebook') and hasattr(self, 'entry_frame'):
                    # Retrieve case number to show in tab title if possible
                    case_num_display = case_data.get('case_number', 'N/A') # Use data directly for case number
                    self.notebook.tab(self.entry_frame, text=f"Edit Case Entry (ID: {case_id} - Case: {case_num_display})")

                logging.info(f"Switched to entry tab to edit case ID: {case_id}")
                self.update_status(f"Editing Case ID: {case_id}")

            else:
                messagebox.showerror("Error", f"Could not retrieve data for case ID {case_id} for editing.")
                logging.error(f"Could not retrieve case data for editing: ID {case_id}")
                self.update_status("Error retrieving case for editing.")

        except Exception as e:
            messagebox.showerror("Edit Error", f"An error occurred while trying to edit the case: {e}")
            logging.exception(f"Error in edit_selected_case for item {selected_item}:")
            self.update_status("Error preparing case for editing.")


    def populate_entry_form(self, case_data):
        """Populates the entry form widgets with data from a case dictionary."""
        # Clear the form first before populating with new data
        self.clear_entry_form() # This also resets editing_case_id, set it again below

        # Temporarily store editing_case_id before clearing, then restore it
        temp_editing_id = self.editing_case_id
        self.editing_case_id = None # Clear before populating, will be set back below if needed


        for key, widget in self.entries.items():
            value = case_data.get(key) # Use .get() to avoid KeyError

            if isinstance(widget, ttk.Entry):
                widget.insert(0, str(value) if value is not None else '') # Insert text
            elif isinstance(widget, tk.StringVar): # Combobox StringVar
                # Find the value in the combobox options and set it
                # This assumes the StringVar is used for comboboxes
                 combo_widget = None
                 # Find the corresponding combobox widget to get its values
                 # We need to iterate through children of field_frame_container to find the actual Combobox widget
                 if hasattr(self, 'field_frame_container') and self.field_frame_container:
                      for child in self.field_frame_container.winfo_children(): # Iterate through cell frames
                          for grandchild in child.winfo_children(): # Iterate through label and widget in cell frame
                              if isinstance(grandchild, ttk.Combobox):
                                   # Check if the textvariable associated with the combobox matches the current entry key
                                   # Use .cget('textvariable') which returns the internal name or string
                                   # Check if the name of the StringVar matches the key
                                   if isinstance(grandchild.cget('textvariable'), str) and grandchild.cget('textvariable') == key:
                                        combo_widget = grandchild
                                        break # Found the combobox
                                   # Also check if the StringVar object itself is the textvariable
                                   if isinstance(widget, tk.StringVar) and grandchild.cget('textvariable') is widget:
                                        combo_widget = grandchild
                                        break # Found the combobox

                          if combo_widget: break # Stop outer loop if found

                 if combo_widget and value is not None:
                      value_str = str(value) # Ensure value is string for comparison
                      current_values = list(combo_widget.cget('values')) # Get combobox options as a list
                      if value_str in current_values:
                         widget.set(value_str)
                      else:
                          logging.warning(f"Value '{value_str}' for {key} not found in combobox options during form population. Setting to default.")
                          if current_values: widget.set(current_values[0]) # Set to first item if available
                          else: widget.set('') # Otherwise set to empty
                 elif isinstance(widget.get(), str): # If it's just a StringVar not linked to a Combobox (less common in this app structure)
                     widget.set(str(value) if value is not None else '')


            elif isinstance(widget, tk.BooleanVar): # Checkbutton BooleanVar
                # Convert "Yes" from DB to True, "No" or other to False
                 widget.set(True if str(value).strip().lower() == 'yes' else False)


            elif isinstance(widget, tk.Text): # Text widget for Notes
                if value is not None:
                     # Delete existing text first before inserting
                     widget.delete('1.0', tk.END)
                     widget.insert(tk.END, str(value))

            # Handle DateEntry widgets separately
            elif isinstance(widget, DateEntry):
                 if value: # value should beYYYY-MM-DD string or similar from DB
                     try:
                         # Attempt to parse the date string from DB
                         date_obj = datetime.strptime(str(value), '%Y-%m-%d').date()
                         widget.set_date(date_obj)
                     except (ValueError, TypeError) as e:
                         logging.warning(f"Could not parse date '{value}' for {key} during form population: {e}. Setting to None.")
                         widget.set_date(None) # Set to None if parsing fails
                 else:
                     widget.set_date(None) # Ensure date field is clear if value is None/empty

            # Add handling for other widget types if any exist in self.entries
            # else:
            #     logging.warning(f"Unknown widget type for key '{key}' during form population: {type(widget)}")


        # Restore editing_case_id after populating if it was set before clearing
        self.editing_case_id = temp_editing_id
        if self.editing_case_id is not None:
             # If we successfully restored an ID, update the UI to show editing state
             if self.submit_button:
                 self.submit_button.config(text="Update Case", style="Accent.TButton")
             if hasattr(self, 'notebook') and hasattr(self, 'entry_frame'):
                  # Retrieve case number to show in tab title if possible
                  case_num_display = case_data.get('case_number', 'N/A') # Use data directly for case number
                  self.notebook.tab(self.entry_frame, text=f"Edit Case Entry (ID: {self.editing_case_id} - Case: {case_num_display})")


    def delete_selected_cases(self):
        """Deletes the selected cases from the database and the Treeview."""
        selected_items = self.tree.selection()
        if not selected_items:
            messagebox.showwarning("No Selection", "Please select one or more cases to delete.")
            logging.warning("Delete failed: No cases selected.")
            return

        # Ask for confirmation
        if messagebox.askyesno("Confirm Deletion", f"ARE YOU SURE you want to delete {len(selected_items)} selected case(s)? This cannot be undone."):
            deleted_count = 0
            failed_count = 0
            logging.info(f"User confirmed deletion of {len(selected_items)} cases.")

            # Iterate through selected items and delete them
            # It's safer to get the list of items first as deleting items changes the selection
            items_to_delete = list(selected_items)
            for item in items_to_delete:
                # Get the database ID of the selected item using its iid
                try:
                    # Ensure case_id is treated as integer for DB operation
                    case_id = int(self.tree.item(item, 'iid'))
                    logging.info(f"Attempting to delete case with ID: {case_id}")

                    if delete_case_db(case_id):
                        deleted_count += 1
                        # Remove the item from the treeview immediately after successful DB deletion
                        self.tree.delete(item)
                        logging.info(f"Successfully deleted case ID {case_id} from DB and Treeview.")
                    else:
                        failed_count += 1
                        logging.error(f"Failed to delete case ID {case_id} from DB.")

                except (ValueError, IndexError, Exception) as e:
                    logging.error(f"Error deleting treeview item {item} or getting its ID: {e}")
                    failed_count += 1

            # Show summary of deletion results
            info_message = f"Deletion complete."
            if deleted_count > 0: info_message += f"\nSuccessfully deleted {deleted_count} case(s)."
            if failed_count > 0: info_message += f"\nFailed to delete {failed_count} case(s). See application log for details."
            messagebox.showinfo("Deletion Complete", info_message)

            logging.info(f"Deletion process finished. Deleted: {deleted_count}, Failed: {failed_count}.")

            # Refresh data view is not strictly necessary if deleting directly from treeview,
            # but update related UI elements.
            # self.refresh_data_view() # Optional: uncomment if direct treeview deletion is removed

            # Update graph filters and map markers as data has changed
            self.update_status("Data deleted. Updating graphs and map...")
            self.populate_graph_filters() # This also calls update_graph
            if hasattr(self, 'map_widget'):
                 self.load_map_markers() # This will start a new background load
            self.update_status("Ready")
        else:
            logging.info("Deletion cancelled by user at confirmation.")
            self.update_status("Deletion cancelled.")


    # --- File Handling (Import/Export/Logo/Marker) ---

    def load_logo_image(self):
        """Loads the logo image from the data directory."""
        logo_path = LOGO_FILENAME
        try:
            if os.path.exists(logo_path):
                # Use PIL to open and resize the image
                img = Image.open(logo_path)

                # Resize for Entry tab (maintain aspect ratio)
                max_height = 100
                img_ratio = img.width / img.height
                new_width = int(img_ratio * max_height)
                img_resized = img.resize((new_width, max_height), Image.Resampling.LANCZOS) # Use LANCZOS for resizing
                self.logo_image_tk = ImageTk.PhotoImage(img_resized)

                # Resize for Settings preview (thumbnail)
                img_preview = Image.open(logo_path) # Re-open original
                img_preview.thumbnail((200, 100), Image.Resampling.LANCZOS) # Use LANCZOS for resizing
                self.logo_image_tk_preview = ImageTk.PhotoImage(img_preview)

                logging.info(f"Loaded logo image from {logo_path}")
            else:
                self.logo_image_tk = None
                self.logo_image_tk_preview = None
                logging.info(f"No logo.png found in {DATA_DIR}. Using default 'No Logo' text.")

        except Exception as e:
            self.logo_image_tk = None
            self.logo_image_tk_preview = None
            logging.error(f"Error loading logo image from {logo_path}: {e}")
            # Avoid messagebox during init, will update UI later
            # messagebox.showerror("Image Error", f"Could not load logo image: {e}\nUsing default 'No Logo'.")

        # Update the logo display in the entry tab if the label exists (called after create_widgets)
        if hasattr(self, 'entry_logo_label') and self.entry_logo_label:
             self.update_entry_logo()
        # Update the logo preview in the settings tab if the canvas exists (called after create_settings_widgets)
        if hasattr(self, 'logo_preview_canvas') and self.logo_preview_canvas:
             self.update_logo_preview()


    def select_logo(self):
        """Opens a file dialog to select a new logo image and saves it."""
        filetypes = [("Image files", "*.png *.jpg *.jpeg *.gif"), ("All files", "*.*")]
        source_path = filedialog.askopenfilename(title="Select Header Logo Image", filetypes=filetypes)

        if not source_path:
            self.update_status("Logo selection cancelled.")
            logging.info("Logo selection cancelled by user.")
            return # User cancelled

        if not os.path.exists(DATA_DIR):
            os.makedirs(DATA_DIR)

        target_path = LOGO_FILENAME

        try:
            # Copy the selected file to the data directory with the standard filename
            shutil.copyfile(source_path, target_path)
            logging.info(f"Copied selected logo to {target_path}")

            # Load the newly selected logo image
            self.load_logo_image()

            # Update the logo preview in the settings tab
            self.update_logo_preview()
            # Update the logo display in the entry tab
            self.update_entry_logo()


            messagebox.showinfo("Logo Updated", f"Logo saved to:\n{target_path}")
            logging.info("Header logo updated.")
            self.update_status("Header logo updated.")

        except Exception as e:
            messagebox.showerror("File Error", f"Failed to save logo image: {e}")
            logging.error(f"Failed to save logo image: {e}")
            self.load_logo_image() # Attempt to load default or clear on error
            self.update_logo_preview() # Attempt to update preview even on error
            self.update_entry_logo() # Attempt to update entry tab even on error
            self.update_status("Failed to save logo image.")


    def update_entry_logo(self):
        """Updates the logo image displayed in the Entry tab."""
        # Ensure the label exists before trying to configure it
        if hasattr(self, 'entry_logo_label') and self.entry_logo_label:
            if self.logo_image_tk:
                # Configure the label to display the image
                self.entry_logo_label.config(image=self.logo_image_tk, text="")
                self.entry_logo_label.image = self.logo_image_tk # Keep a reference!
                # Repack or re-place the label if needed (depends on initial layout)
                # self.entry_logo_label.pack(side='right', anchor='ne', pady=5) # Assuming pack is used
            else:
                # If no logo image is loaded, show "No Logo" text
                self.entry_logo_label.config(image='', text="No Logo")
                self.entry_logo_label.image = None # Clear image reference
                # Repack or re-place the label if needed
                # self.entry_logo_label.pack(side='right', anchor='ne', pady=5) # Assuming pack is used


    def update_logo_preview(self):
        """Updates the logo image displayed in the Settings tab preview."""
        # Ensure the canvas exists before trying to update it
        if hasattr(self, 'logo_preview_canvas') and self.logo_preview_canvas:
            self.logo_preview_canvas.delete("all")

            # Use the separate preview image variable
            if self.logo_image_tk_preview:
                 # Center the image on the canvas (width 200, height 100)
                 self.logo_preview_canvas.create_image(100, 50, anchor='center', image=self.logo_image_tk_preview)
            else:
                self.logo_preview_canvas.create_text(100, 50, anchor='center', text="No Logo Set")


    def load_marker_icon_image(self):
        """Loads the custom marker icon image, falls back to default if not found."""
        custom_icon_path = MARKER_ICON_FILENAME
        default_icon_path = os.path.join(DATA_DIR, "marker_pin.png") # Default pin location
        loaded_successfully = False
        global DEFAULT_MARKER_ICON # Access the global variable used by tkintermapview

        # Try loading the custom icon first
        if os.path.exists(custom_icon_path):
            try:
                # Use PIL to open and resize the image
                img_map = Image.open(custom_icon_path)
                img_map = img_map.resize((20, 20), Image.Resampling.LANCZOS) # Use LANCZOS for resizing
                self.marker_icon_tk_map = ImageTk.PhotoImage(img_map)

                # Load and resize for settings preview (e.g., 50x50 thumbnail)
                img_preview = Image.open(custom_icon_path) # Re-open original
                img_preview.thumbnail((50, 50), Image.Resampling.LANCZOS) # Example size for preview
                self.marker_icon_tk_preview = ImageTk.PhotoImage(img_preview)

                DEFAULT_MARKER_ICON = self.marker_icon_tk_map # Set global for map view to the custom icon
                logging.info(f"Loaded custom marker icon from {custom_icon_path}")
                loaded_successfully = True
            except Exception as e:
                logging.error(f"Error loading custom marker icon from {custom_icon_path}: {e}")
                self.marker_icon_tk_map = None
                self.marker_icon_tk_preview = None
                DEFAULT_MARKER_ICON = None # Fallback to None if custom fails

        # If custom icon failed to load or didn't exist, try loading the default marker_pin.png
        if not loaded_successfully and os.path.exists(default_icon_path):
             try:
                 # Use PIL to open and resize the image
                 img_map = Image.open(default_icon_path)
                 img_map = img_map.resize((20, 20), Image.Resampling.LANCZOS) # Use LANCZOS for resizing
                 self.marker_icon_tk_map = ImageTk.PhotoImage(img_map)

                 # Load and resize for settings preview
                 img_preview = Image.open(default_icon_path) # Re-open original
                 img_preview.thumbnail((50, 50), Image.Resampling.LANCZOS) # Example size for preview
                 self.marker_icon_tk_preview = ImageTk.PhotoImage(img_preview)

                 DEFAULT_MARKER_ICON = self.marker_icon_tk_map # Set global for map view to the default icon
                 logging.info(f"Loaded default marker icon from {default_icon_path}")
                 loaded_successfully = True
             except Exception as e:
                 logging.error(f"Error loading default marker icon from {default_icon_path}: {e}")
                 self.marker_icon_tk_map = None
                 self.marker_icon_tk_preview = None
                 DEFAULT_MARKER_ICON = None # Fallback to None if default fails

        # If neither loaded, ensure DEFAULT_MARKER_ICON is None so tkintermapview uses its built-in default
        if not loaded_successfully:
             logging.warning("No custom or default marker icon found. Using tkintermapview default.")
             self.marker_icon_tk_map = None
             self.marker_icon_tk_preview = None
             DEFAULT_MARKER_ICON = None # Ensure global is None if no icon is loaded

        # Update the marker icon preview in the settings tab if the canvas exists (called after create_settings_widgets)
        if hasattr(self, 'marker_icon_preview_canvas') and self.marker_icon_preview_canvas:
             self.update_marker_icon_preview()


    def select_marker_icon(self):
        """Opens a file dialog to select a new marker icon image and saves it."""
        filetypes = [("Image files", "*.png *.jpg *.jpeg *.gif"), ("All files", "*.*")]
        source_path = filedialog.askopenfilename(title="Select Map Marker Image", filetypes=filetypes)

        if not source_path:
            self.update_status("Marker icon selection cancelled.")
            logging.info("Marker icon selection cancelled by user.")
            return # User cancelled

        if not os.path.exists(DATA_DIR):
            os.makedirs(DATA_DIR)

        target_path = MARKER_ICON_FILENAME

        try:
            # Copy the selected file to the data directory with the standard filename
            shutil.copyfile(source_path, target_path)
            logging.info(f"Copied selected marker icon to {target_path}")

            # Load the newly selected marker icon image
            self.load_marker_icon_image()

            # Update the marker icon preview in the settings tab
            self.update_marker_icon_preview()

            # Reload map markers to use the new icon if map widget exists
            # This should now trigger the threaded loading process
            if hasattr(self, 'map_widget') and self.map_widget:
                 self.update_status("Reloading map markers with new icon...")
                 self.load_map_markers() # This calls load_map_markers which starts the thread


            messagebox.showinfo("Marker Icon Updated", f"Marker icon saved to:\n{target_path}")
            logging.info("Map marker icon updated successfully.")
            # update_status is called by load_map_markers

        except Exception as e:
            messagebox.showerror("File Error", f"Failed to save marker icon: {e}")
            logging.error(f"Failed to save marker icon: {e}")
            self.load_marker_icon_image() # Attempt to load default or clear on error
            self.update_marker_icon_preview() # Attempt to update preview even on error
            self.update_status("Failed to save marker icon.")


    def update_marker_icon_preview(self):
        """Updates the marker icon image displayed in the Settings tab preview."""
        # Ensure the canvas exists before trying to update it
        if hasattr(self, 'marker_icon_preview_canvas') and self.marker_icon_preview_canvas:
            self.marker_icon_preview_canvas.delete("all")

            # Use the separate preview image variable (50x50)
            if self.marker_icon_tk_preview:
                 # Center the image on the 50x50 canvas
                 self.marker_icon_preview_canvas.create_image(25, 25, anchor='center', image=self.marker_icon_tk_preview)
            else:
                # Display text if no custom or default icon is loaded
                self.marker_icon_preview_canvas.create_text(25, 25, anchor='center', text="No Icon Set\n(Using Default)") # Adjusted text


    def export_pdf_report(self):
        """Exports all case data to a PDF file."""
        file_path = filedialog.asksaveasfilename(defaultext=".pdf",
                                                 filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")],
                                                 title="Save PDF Report")
        if not file_path:
            self.update_status("PDF export cancelled.")
            logging.info("PDF export cancelled by user.")
            return # User cancelled

        self.update_status(f"Exporting to {os.path.basename(file_path)}...")
        self.root.update_idletasks()

        cases = get_all_cases_db()
        if not cases:
             messagebox.showinfo("Export Info", "No cases to export.")
             self.update_status("PDF export: No cases to export.")
             logging.info("PDF export cancelled: No cases to export.")
             return

        try:
            # Use landscape orientation for wider table
            doc = SimpleDocTemplate(file_path, pagesize=landscape(letter))
            story = []
            styles = getSampleStyleSheet()

            # Add title
            title = Paragraph(f"{APP_NAME} - Case Log Report", styles['h1'])
            story.append(title)
            story.append(Spacer(1, 0.2*inch)) # Add some space

            # Add logo if available
            if self.logo_image_tk:
                 # Need to load the logo image using ReportLab's Image class
                 # Save the PhotoImage to a temporary buffer to be read by ReportLab
                 try:
                     buffer = io.BytesIO()
                     # Get the PIL Image object from the PhotoImage
                     # This is a bit hacky, relies on internal structure of ImageTk.PhotoImage
                     # A safer way is to store/pass the PIL Image object directly or reload from path
                     # Let's reload from path as it's more robust
                     if os.path.exists(LOGO_FILENAME):
                          pil_img = Image.open(LOGO_FILENAME)
                          # Resize for report (adjust as needed)
                          img_ratio = pil_img.width / pil_img.height
                          report_logo_width = 1.5 * inch # Desired width in report
                          report_logo_height = report_logo_width / img_ratio
                          pil_img_resized = pil_img.resize((int(report_logo_width), int(report_logo_height)), Image.Resampling.LANCZOS)

                          pil_img_resized.save(buffer, format='PNG')
                          buffer.seek(0) # Rewind the buffer
                          reportlab_logo = ReportLabImage(buffer)

                          # Set the size ReportLab should draw it at
                          reportlab_logo.drawWidth = report_logo_width
                          reportlab_logo.drawHeight = report_logo_height

                          story.append(reportlab_logo)
                          story.append(Spacer(1, 0.2*inch))
                     else:
                          logging.warning("Logo file not found at LOGO_FILENAME for PDF export.")

                 except Exception as e:
                      logging.error(f"Error embedding logo in PDF: {e}")
                      # Continue with PDF export even if logo fails


            # Prepare data for the table
            # Define columns to include and their order for the PDF, excluding 'id' and 'created_at' timestamp
            # This needs to match the desired report structure
            pdf_columns_order = [
                 "case_number", "examiner", "investigator", "agency", "city_of_offense",
                 "state_of_offense", "start_date", "end_date", "volume_size_gb",
                 "offense_type", "device_type", "model", "os", "data_recovered",
                 "fpr_complete", "notes"
            ]

            # Use display text for headers
            header_row = [self.tree_columns_config.get(col_key, {}).get("text", col_key) for col_key in pdf_columns_order]
            data = [header_row]

            for case in cases:
                row_data = []
                for col_key in pdf_columns_order:
                    value = case.get(col_key, '') # Get value, default to empty string

                    # Format data for PDF display
                    if col_key in ['start_date', 'end_date']:
                        formatted_value = format_date_str_for_display(value)
                    elif col_key == 'fpr_complete':
                        formatted_value = format_bool_int(value)
                    elif col_key == 'volume_size_gb' and value is not None:
                        formatted_value = str(value)
                    else:
                        formatted_value = str(value) # Ensure all data is string

                    row_data.append(formatted_value)
                data.append(row_data)

            # Create the table
            table = Table(data)

            # Add TableStyle
            style = TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey), # Header row background
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke), # Header row text color
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'), # Align all text to the left
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'), # Header font
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12), # Header padding
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige), # Alternating row background
                ('GRID', (0, 0), (-1, -1), 1, colors.black), # Add grid lines
                ('BOX', (0, 0), (-1, -1), 1, colors.black), # Add box around table
                ('WORDWRAP', (0, 1), (-1, -1), True), # Enable word wrapping for data rows
                ('VALIGN', (0, 0), (-1, -1), 'TOP'), # Align text to top in cells
            ])
            table.setStyle(style)

            # Calculate column widths dynamically based on approximate text length or set fixed widths
            # This is a simple approach; a more robust solution would analyze content
            # Let's try setting some default widths and adjust notes
            num_cols = len(header_row)
            # Ensure landscape(letter)[0] is used for width
            page_width, page_height = landscape(letter)
            margin = inch # Example margin
            usable_width = page_width - 2 * margin
            default_col_width = usable_width / num_cols # Total width minus margins, divided by columns
            col_widths = [default_col_width] * num_cols

            # Adjust width for Notes column
            try:
                 notes_col_index = pdf_columns_order.index("notes")
                 col_widths[notes_col_index] = 2.5*inch # Give Notes a wider column
                 # Redistribute the lost width among other columns if needed, or just let ReportLab handle it
                 remaining_width_for_other_cols = usable_width - col_widths[notes_col_index]
                 num_other_cols = num_cols - 1
                 if num_other_cols > 0:
                      new_other_col_width = remaining_width_for_other_cols / num_other_cols
                      for i in range(num_cols):
                          if i != notes_col_index:
                              col_widths[i] = new_other_col_width


            except ValueError:
                 logging.warning("Notes column not found in pdf_columns_order for width adjustment.")


            table._argW = col_widths # Assign calculated widths


            story.append(table)

            # Build the PDF
            doc.build(story)

            messagebox.showinfo("Export Complete", f"PDF report saved successfully to:\n{file_path}")
            logging.info(f"PDF report exported successfully to {file_path}")
            self.update_status("PDF export complete.")

        except Exception as e:
            messagebox.showerror("Export Error", f"Failed to export PDF report: {e}")
            logging.exception("Error exporting PDF report:")
            self.update_status("PDF export failed.")


    def export_xlsx_report(self):
        """Exports all case data to an XLSX file."""
        file_path = filedialog.asksaveasfilename(defaultext=".xlsx",
                                                 filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")],
                                                 title="Save XLSX Report")
        if not file_path:
            self.update_status("XLSX export cancelled.")
            logging.info("XLSX export cancelled by user.")
            return # User cancelled

        self.update_status(f"Exporting to {os.path.basename(file_path)}...")
        self.root.update_idletasks()

        cases = get_all_cases_db()
        if not cases:
             messagebox.showinfo("Export Info", "No cases to export.")
             self.update_status("XLSX export: No cases to export.")
             logging.info("XLSX export cancelled: No cases to export.")
             return

        try:
            # Convert list of dictionaries to pandas DataFrame
            df = pd.DataFrame(cases)

            # Reorder columns to match the desired export order (similar to PDF, include internal 'id')
            xlsx_columns_order = [
                 "id", "case_number", "examiner", "investigator", "agency", "city_of_offense",
                 "state_of_offense", "start_date", "end_date", "volume_size_gb",
                 "offense_type", "device_type", "model", "os", "data_recovered",
                 "fpr_complete", "notes", "created_at" # Include created_at in XLSX
            ]
            # Ensure all columns in order are present in the DataFrame (add if missing, with NaN values)
            # Use .reindex to select and reorder columns
            df = df.reindex(columns=xlsx_columns_order)


            # Rename columns for the header row using display text from tree_columns_config
            # Create a mapping from original DB column key to desired Excel header text
            # Use the display text from tree_columns_config where available, otherwise use the key
            rename_dict = {col_key: self.tree_columns_config.get(col_key, {}).get("text", col_key)
                           for col_key in xlsx_columns_order} # Map all ordered columns

            df.rename(columns=rename_dict, inplace=True)

            # Format specific columns for XLSX output if needed (e.g., dates, booleans)
            # Pandas often handles basic types reasonably well for Excel
            # Ensure date columns are datetime objects or strings in<\ctrl97>MM-DD format
            for original_col_key in ['start_date', 'end_date', 'created_at']:
                 # Find the potentially renamed column name in the DataFrame
                 current_col_name = rename_dict.get(original_col_key, original_col_key)
                 if current_col_name in df.columns:
                     # ConvertYYYY-MM-DD strings back to datetime objects for Excel to recognize them as dates
                     # errors='coerce' will turn unparseable dates into NaT (Not a Time)
                     df[current_col_name] = pd.to_datetime(df[current_col_name], errors='coerce')

            # Convert 0/1 for fpr_complete back to True/False or "Yes"/"No" if preferred for export
            # Use the original DB column key to find the column before renaming
            original_fpr_col = 'fpr_complete'
            if original_fpr_col in df.columns:
                 # Use the renamed column name for the display header if available
                 display_col_name = rename_dict.get(original_fpr_col, original_fpr_col)
                 if display_col_name in df.columns:
                     # Convert 0/1 to True/False
                     df[display_col_name] = df[display_col_name].astype(bool)

            # Convert "Yes"/"No"/"" for Data Recovered to True/False/None if preferred for export
            original_dr_col = 'data_recovered'
            if original_dr_col in df.columns:
                 display_col_name = rename_dict.get(original_dr_col, original_dr_col)
                 if display_col_name in df.columns:
                     # Map "Yes" -> True, "No" -> False, ""/None -> None
                     df[display_col_name] = df[display_col_name].map({'Yes': True, 'No': False, '': None, None: None})


            # Write DataFrame to Excel file
            df.to_excel(file_path, index=False) # index=False to not write the DataFrame index


            messagebox.showinfo("Export Complete", f"XLSX report saved successfully to:\n{file_path}")
            logging.info(f"XLSX report exported successfully to {file_path}")
            self.update_status("XLSX export complete.")

        except Exception as e:
            messagebox.showerror("Export Error", f"Failed to export XLSX report: {e}")
            logging.exception("Error exporting XLSX report:")
            self.update_status("XLSX export failed.")


    def import_cases_from_xlsx(self):
        """Imports case data from a selected XLSX file, handling duplicates and updates."""
        file_path = filedialog.askopenfilename(title="Select XLSX File", filetypes=[("Excel files", "*.xlsx")])
        if not file_path:
            self.update_status("XLSX import cancelled.")
            logging.info("XLSX import cancelled by user.")
            return # User cancelled

        logging.info(f"Attempting to import cases from XLSX: {file_path}")
        self.update_status(f"Importing from {os.path.basename(file_path)}...")
        self.root.update_idletasks() # Update status bar immediately

        try:
            # Read the Excel file into a pandas DataFrame
            df = pd.read_excel(file_path, engine='openpyxl')

            # Define the columns we expect in the XLSX and the corresponding DB keys
            # This dictionary maps Excel Column Header -> Database Key
            # This makes the import more robust to changes in Excel header names
            # You'll need to adjust the keys on the left to match your actual Excel file headers
            # For now, assuming Excel headers match Treeview Display Text
            excel_header_to_db_key = {
                config.get("text", col_key): col_key # Use display text as key, db key as value
                for col_key, config in self.tree_columns_config.items()
                if col_key not in ['id', 'created_at'] # Exclude internal DB fields and generated fields
            }
             # Manually add Case # and other critical ones if display text differs from common usage
             # Ensure 'Case #' is mapped to 'case_number' etc.
            excel_header_to_db_key.update({
                "Case #": "case_number",
                "Examiner": "examiner",
                "Investigator": "investigator",
                "Agency": "agency",
                "City": "city_of_offense",
                "State": "state_of_offense",
                "Start (MM-DD-YYYY)": "start_date", # Adjust if your Excel uses different date format header
                "End (MM-DD-YYYY)": "end_date",     # Adjust if your Excel uses different date format header
                "Vol (GB)": "volume_size_gb",
                "Offense": "offense_type",
                "Device": "device_type",
                "Model": "model",
                "OS": "os",
                "Recovered?": "data_recovered",
                "FPR?": "fpr_complete",
                "Notes": "notes"
            })


            imported_count = 0
            updated_count = 0
            skipped_count = 0 # Count cases skipped due to no changes or missing case number
            failed_cases = [] # List to store info about rows that failed to process

            total_rows = len(df)
            if total_rows == 0:
                messagebox.showinfo("Import Info", "The selected Excel file is empty.")
                self.update_status("XLSX import failed: Empty file.")
                logging.warning("XLSX Import Error: Empty file.")
                return


            for index, row in df.iterrows():
                # Convert row to a dictionary based on the excel_header_to_db_key map
                case_data_from_xlsx = {}
                for excel_col_header, db_key in excel_header_to_db_key.items():
                     # Use .get() on the row with the Excel column header
                     # Also handle potential renaming in Excel headers
                     value = None
                     if excel_col_header in row:
                          value = row[excel_col_header]
                     elif db_key in row: # Fallback to DB key name if display header not found
                          value = row[db_key]
                     else:
                          logging.debug(f"Column '{excel_col_header}' or DB key '{db_key}' not found in XLSX row {index+2}.")
                          case_data_from_xlsx[db_key] = None # Set to None if column is missing
                          continue # Move to next db_key

                     if pd.isna(value):
                         case_data_from_xlsx[db_key] = None
                     elif isinstance(value, str):
                         case_data_from_xlsx[db_key] = value.strip()
                     else:
                         # Keep numeric, boolean, datetime objects as is for now, handle below
                         case_data_from_xlsx[db_key] = value


                # --- Data Type Conversions and Validation (matching submit_case logic) ---

                # Handle 'case_number' - required field
                case_number = case_data_from_xlsx.get('case_number')
                if not case_number or not str(case_number).strip():
                    logging.warning(f"Skipping row {index+2} due to missing Case Number.")
                    skipped_count += 1
                    self.update_status(f"Importing row {index + 2} of {total_rows}... (Skipped: Missing Case #)")
                    self.root.update_idletasks()
                    continue # Skip rows with no case number
                case_number = str(case_number).strip() # Ensure case number is stripped string


                # Handle 'fpr_complete' - convert to boolean for comparison/insert logic later
                fpr_val = str(case_data_from_xlsx.get('fpr_complete', '')).strip().lower()
                # Convert common representations to boolean
                case_data_from_xlsx['fpr_complete'] = True if fpr_val in ['true', '1', 'yes'] else False # Convert to Boolean


                # Handle 'volume_size_gb' conversion to float or None
                vol_val = case_data_from_xlsx.get('volume_size_gb')
                if vol_val is not None and str(vol_val).strip() != '': # Check if not None and not empty string representation
                    try:
                        # Attempt to convert to float
                        case_data_from_xlsx['volume_size_gb'] = float(str(vol_val).strip())
                    except ValueError:
                        logging.warning(f"Invalid volume_size_gb for row {index+2} (Case #: {case_number}): '{vol_val}'. Setting to None.")
                        case_data_from_xlsx['volume_size_gb'] = None
                    except TypeError: # Handle other types that might cause errors
                         logging.warning(f"Unexpected type for volume_size_gb in row {index+2} (Case #: {case_number}): {type(vol_val)}. Setting to None.")
                         case_data_from_xlsx['volume_size_gb'] = None
                else:
                    case_data_from_xlsx['volume_size_gb'] = None # Ensure explicit None for empty/NaN


                # Handle 'data_recovered' - convert to "Yes", "No", or ""
                dr_val = str(case_data_from_xlsx.get('data_recovered', '')).strip().capitalize()
                if dr_val not in ["Yes", "No", ""]:
                     logging.warning(f"Unexpected 'data_recovered' value '{dr_val}' for row {index+2} (Case #: {case_number}): Setting to empty.");
                     dr_val = "" # Default to empty if unexpected value
                case_data_from_xlsx['data_recovered'] = dr_val


                # Handle date conversions toYYYY-MM-DD strings or None
                for date_key in ['start_date', 'end_date']:
                    date_val = case_data_from_xlsx.get(date_key)
                    if isinstance(date_val, (datetime, pd.Timestamp)):
                         # If pandas read it as datetime, format it
                        case_data_from_xlsx[date_key] = date_val.strftime('%Y-%m-%d')
                    elif isinstance(date_val, str) and date_val.strip(): # Process non-empty strings
                        parsed_date = None
                        # Attempt to parse various date string formats, prioritize MM-DD-YYYY as in assumed Excel header
                        for fmt in ('%m-%d-%Y', '%m/%d/%Y', '%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%m/%d/%Y %H:%M:%S'):
                            try:
                                parsed_date = datetime.strptime(date_val.strip(), fmt)
                                break # Stop on first successful parse
                            except ValueError:
                                continue
                        if parsed_date:
                            case_data_from_xlsx[date_key] = parsed_date.strftime('%Y-%m-%d')
                        else:
                            logging.warning(f"Unparseable date string '{date_val}' for {date_key} in row {index+2} (Case #: {case_number}). Setting to None.")
                            case_data_from_xlsx[date_key] = None
                    elif date_val is not None: # Handle non-string, non-datetime types that aren't None
                         logging.warning(f"Unexpected type for {date_key} in row {index+2} (Case #: {case_number}): {type(date_val)}. Setting to None.")
                         case_data_from_xlsx[date_key] = None
                    else:
                         case_data_from_xlsx[date_key] = None # Ensure explicit None for empty/NaN


                self.update_status(f"Importing row {index + 2} of {total_rows}... (Processing Case #: {case_number})")
                self.root.update_idletasks()

                # Check if case exists in DB by case_number
                existing_case = get_case_by_number_db(case_number)

                if existing_case:
                    # Case exists, check for changes
                    changes_found = False
                    update_data = {}
                    # Use the keys from the excel_header_to_db_key values (DB keys)
                    db_keys_to_compare = [db_key for db_key in excel_header_to_db_key.values() if db_key not in ['id', 'created_at', 'case_number']]

                    for db_key in db_keys_to_compare:
                         imported_value = case_data_from_xlsx.get(db_key)
                         existing_value = existing_case.get(db_key)

                         # --- Comparison Logic ---
                         # Need careful comparison based on expected data types in the DB
                         if db_key == 'fpr_complete':
                              # Compare boolean (from XLSX processing) with integer (from DB)
                             if (imported_value is True and existing_value != 1) or (imported_value is False and existing_value != 0):
                                 changes_found = True
                                 update_data[db_key] = 1 if imported_value else 0 # Store as integer 0/1
                         elif db_key == 'volume_size_gb':
                              # Compare floats carefully, handle None
                             imported_float = float(imported_value) if imported_value is not None else None
                             existing_float = float(existing_value) if existing_value is not None else None

                             if (imported_float is None and existing_float is not None) or \
                                (imported_float is not None and existing_float is None) or \
                                (imported_float is not None and existing_float is not None and abs(imported_float - existing_float) > 1e-9): # Using tolerance for float comparison
                                changes_found = True
                                update_data[db_key] = imported_float # Store as float or None
                         elif db_key in ['start_date', 'end_date']:
                             # Compare date strings (YYYY-MM-DD or None)
                             # Ensure comparison handles None and empty strings consistently
                             imported_date_str = imported_value if imported_value else None # Treat "" as None for dates
                             existing_date_str = existing_value if existing_value else None # Treat "" as None for dates

                             if imported_date_str != existing_date_str:
                                changes_found = True
                                update_data[db_key] = imported_date_str # Store asYYYY-MM-DD string or None
                         elif db_key == 'data_recovered':
                             # Compare "Yes"/"No"/"" strings
                             imported_dr_str = str(imported_value).strip().capitalize() if imported_value is not None else ""
                             existing_dr_str = str(existing_value).strip().capitalize() if existing_value is not None else ""

                             if imported_dr_str != existing_dr_str:
                                 changes_found = True
                                 update_data[db_key] = imported_dr_str # Store as "Yes", "No", or ""

                         else:
                             # Standard string comparison for text fields
                             # Ensure both are treated as strings or None for comparison
                             imported_str = str(imported_value).strip() if imported_value is not None else ''
                             existing_str = str(existing_value).strip() if existing_value is not None else ''

                             if imported_str != existing_str:
                                changes_found = True
                                update_data[db_key] = imported_str # Store as string


                    if changes_found:
                        # Update the existing case using its ID
                        # The update_case_db function handles converting boolean fpr_complete to 0/1
                        if update_case_db(existing_case['id'], update_data):
                            updated_count += 1
                            logging.info(f"Updated case {case_number} (ID: {existing_case['id']}) from XLSX.")
                        else:
                            failed_cases.append(f"Row {index+2} (Case #: {case_number}) - Update Failed")
                            logging.error(f"Failed to update case {case_number} (ID: {existing_case['id']}) from XLSX.")
                    else:
                        # No significant changes, skip
                        skipped_count += 1
                        logging.debug(f"Skipping case {case_number} from XLSX: No changes detected.")
                else:
                    # Case does not exist, add as new
                    # The add_case_db function expects fpr_complete as boolean, which is already handled
                    # data_recovered is also handled now
                    # Ensure case_number is included in data for insert
                    case_data_for_insert = case_data_from_xlsx.copy()
                    case_data_for_insert['case_number'] = case_number # Add case_number to data for insert


                    if add_case_db(case_data_for_insert): # add_case_db returns True/False
                        imported_count += 1
                        logging.info(f"Imported new case {case_number} from XLSX.")
                    else:
                        # add_case_db logs the reason (e.g., duplicate if somehow missed get_case_by_number_db)
                        failed_cases.append(f"Row {index+2} (Case #: {case_number}) - Insert Failed")


            # --- Import Summary Message ---
            info_message = f"XLSX Import Summary:"
            info_message += f"\nTotal rows processed: {total_rows}"
            if imported_count > 0: info_message += f"\nSuccessfully imported {imported_count} new case(s)."
            if updated_count > 0: info_message += f"\nSuccessfully updated {updated_count} existing case(s)."
            if skipped_count > 0: info_message += f"\nSkipped {skipped_count} case(s) (already exist with no changes or missing case number)."
            if failed_cases:
                info_message += f"\nEncountered {len(failed_cases)} row(s) that failed to process. Check application log (app.log) for details."
                logging.warning(f"XLSX Import: Encountered {len(failed_cases)} rows that failed to process. Details for first few: {'; '.join(failed_cases[:5])}...")
            else:
                 info_message += "\nNo rows failed to process."


            messagebox.showinfo("Import Complete", info_message)
            logging.info(f"XLSX Import complete. Imported: {imported_count}, Updated: {updated_count}, Skipped: {skipped_count}, Failures: {len(failed_cases)}")

            # --- UI Refresh after Import ---
            self.update_status("Import complete. Refreshing data...")
            self.root.update_idletasks() # Update status bar immediately

            self.refresh_data_view(); # Refresh Treeview

            # Reload map markers and graphs as data has changed.
            # load_map_markers will start the threaded process.
            if hasattr(self, 'map_widget'):
                self.load_map_markers()

            self.populate_graph_filters() # Update graph filters and graph

            self.update_status("Ready")

        except FileNotFoundError:
            messagebox.showerror("Import Error", "File not found.");
            logging.error("XLSX Import Error: File not found.")
            self.update_status("XLSX import failed: File not found.")
        except pd.errors.EmptyDataError:
            messagebox.showerror("Import Error", "The selected Excel file is empty.");
            logging.error("XLSX Import Error: Empty file.")
            self.update_status("XLSX import failed: Empty file.")
        except Exception as e:
            messagebox.showerror("Import Error", f"An unexpected error occurred during XLSX import: {e}");
            logging.exception("Unexpected XLSX Import Error:")
            self.update_status(f"XLSX import failed: {e}")


    def show_application_log(self):
        """Displays the application log in a new window."""
        if not os.path.exists(LOG_FILENAME):
            messagebox.showinfo("Log Info", "Application log file not found.")
            return

        log_window = tk.Toplevel(self.root)
        log_window.title("Application Log")
        log_window.geometry("800x600")

        log_text = scrolledtext.ScrolledText(log_window, wrap=tk.WORD)
        log_text.pack(fill='both', expand=True, padx=10, pady=10)

        try:
            with open(LOG_FILENAME, 'r') as f:
                log_content = f.read()
            log_text.insert(tk.END, log_content)
            log_text.see(tk.END) # Scroll to the bottom
        except Exception as e:
            log_text.insert(tk.END, f"Error reading log file: {e}")


    def change_password_prompt(self):
        """Prompts the user to change the application password."""
        current_password = simpledialog.askstring("Change Password", "Enter current password:", show='*')

        if current_password is None:
            logging.info("Change password cancelled at current password prompt.")
            self.update_status("Password change cancelled.")
            return # User cancelled

        if verify_password(current_password):
            new_password = simpledialog.askstring("Change Password", "Enter new password:", show='*')
            if new_password is None:
                logging.info("Change password cancelled at new password prompt.")
                self.update_status("Password change cancelled.")
                return # User cancelled
            if new_password:
                confirm_password = simpledialog.askstring("Change Password", "Confirm new password:", show='*')
                if confirm_password is None:
                    logging.info("Change password cancelled at confirm password prompt.")
                    self.update_status("Password change cancelled.")
                    return # User cancelled

                if new_password == confirm_password:
                    if update_password_db(new_password):
                        messagebox.showinfo("Success", "Password updated successfully!")
                        logging.info("Application password updated.")
                        self.update_status("Password updated.")
                    else:
                        messagebox.showerror("Error", "Failed to update password in database. See log for details.")
                        logging.error("Failed to update password in database.")
                        self.update_status("Password change failed.")
                else:
                    messagebox.showwarning("Password Mismatch", "New passwords do not match.")
                    logging.warning("Password change failed: New passwords do not match.")
                    self.update_status("Password change failed: Mismatch.")
            else:
                messagebox.showwarning("Invalid Password", "New password cannot be empty.")
                logging.warning("Password change failed: New password empty.")
                self.update_status("Password change failed: Empty new password.")
        else:
            messagebox.showerror("Authentication Failed", "Current password incorrect.")
            logging.warning("Password change failed: Incorrect current password.")
            self.update_status("Password change failed: Authentication failed.")


    def clear_application_data_prompt(self):
        """Prompts for password and clears all application data."""
        logging.warning("Clear Application Data initiated by user.")
        password = simpledialog.askstring("Password Required", "Enter password to clear ALL data:", show='*')

        if password is None:
            logging.info("Data clear cancelled by user at password prompt.")
            self.update_status("Data clear cancelled.")
            return # User cancelled

        if verify_password(password):
            if messagebox.askyesno("Confirm Clear Data", "ARE YOU SURE you want to delete ALL application data (database, logo, and marker icon)? This cannot be undone."): # Updated message
                try:
                    self.update_status("Clearing application data...")
                    self.root.update_idletasks() # Update status bar immediately

                    # Delete the database file
                    if os.path.exists(DB_FILENAME):
                        os.remove(DB_FILENAME)
                        logging.info(f"Database file '{DB_FILENAME}' deleted.")
                    # Delete the main logo file
                    if os.path.exists(LOGO_FILENAME):
                         os.remove(LOGO_FILENAME)
                         logging.info(f"Main logo file '{LOGO_FILENAME}' deleted.")
                    # Delete the marker icon file
                    if os.path.exists(MARKER_ICON_FILENAME):
                         os.remove(MARKER_ICON_FILENAME)
                         logging.info(f"Marker icon file '{MARKER_ICON_FILENAME}' deleted.")

                    # Optional: Delete the log file as well? Be careful with this.
                    # if os.path.exists(LOG_FILENAME):
                    #      try: os.remove(LOG_FILENAME); logging.info("Log file deleted.")
                    #      except Exception as e: logging.error(f"Failed to delete log file: {e}")


                    init_db() # Re-initialize empty database (will also set default password hash/salt)

                    self.update_status("Data cleared. Refreshing UI...")
                    self.refresh_data_view(); # Refresh Treeview (will be empty)
                    self.populate_graph_filters() # Update graph filters (will be empty)

                    # Clear map markers and reset view
                    if hasattr(self, 'map_widget') and self.map_widget and self.map_widget.winfo_exists():
                         self.map_widget.delete_all_marker() # Clear map markers
                         self.map_status_label.config(text="Map status: Data cleared.")
                         # Reset map view
                         self.map_widget.set_position(32.7, -89.5) # Center on MS
                         self.map_widget.set_zoom(7)


                    # Update logo and marker icon displays after clearing
                    self.load_logo_image() # Re-load potential default logo (if exists)
                    # The load methods now update previews/entry tab if widgets exist
                    # self.update_logo_preview()
                    # self.update_entry_logo()

                    # self.load_marker_icon_image()
                    # self.update_marker_icon_preview()

                    # Clear the entry form as well
                    self.clear_entry_form()


                    messagebox.showinfo("Data Cleared", "Application data has been cleared.")
                    logging.info("Application data cleared successfully.")
                    self.update_status("Application data cleared.")

                except Exception as e:
                    messagebox.showerror("Error Clearing Data", f"Could not clear all data: {e}")
                    logging.exception("Error during data clear:")
                    self.update_status("Error clearing data.")
            else:
                logging.info("Data clear cancelled by user at confirmation.")
                self.update_status("Data clear cancelled.")
        else:
            messagebox.showerror("Incorrect Password", "Password incorrect.")
            logging.warning("Incorrect password entered for data clear.")
            self.update_status("Data clear failed: Incorrect password.")


    # --- Treeview Sorting ---
    # Helper function for sorting treeview columns (numeric, date, boolean, text)
    def sort_treeview_column(self, col, initial_sort=False):
        """Sorts the treeview by the specified column."""
        # Get data from the treeview items
        data = [(self.tree.set(child, col), child) for child in self.tree.get_children('')]

        # Determine the sort order (ascending or descending)
        # If it's the same column as the previous sort, reverse the order
        if col == self.treeview_sort_column and not initial_sort:
            self.treeview_sort_reverse = not self.treeview_sort_reverse
        else:
            # Otherwise, it's a new column, default to ascending
            self.treeview_sort_reverse = False

        # Keep track of the currently sorted column
        self.treeview_sort_column = col

        # Determine the data type for sorting based on tree_columns_config
        col_config = self.tree_columns_config.get(col, {})
        col_type = col_config.get("type", "text")

        # Sort the data based on the column type
        if col_type == "numeric":
            # Sort numerically, handling empty strings or non-numeric values by treating them as 0 or infinity
            # Use a custom key that attempts conversion and handles errors
            def numeric_sort_key(item):
                value_str = str(item[0]).strip()
                if value_str:
                    try:
                        return float(value_str)
                    except ValueError:
                        return float('inf') # Treat non-numeric as largest for sorting
                else:
                    return float('-inf') # Treat empty as smallest
            data.sort(key=numeric_sort_key, reverse=self.treeview_sort_reverse)

        elif col_type == "date":
            # Sort dates, handling empty strings or unparseable dates
            def date_sort_key(item):
                date_str = item[0] # Date is already in MM-DD-YYYY display format
                if date_str:
                    try:
                        # Attempt to parse MM-DD-YYYY format from display
                        return datetime.strptime(date_str, '%m-%d-%Y').date()
                    except ValueError:
                        # If parsing fails, return a very early date or None to keep them together
                        return datetime.min.date() # Treat invalid dates as very early
                else:
                    return datetime.min.date() # Treat empty dates as very early
            data.sort(key=date_sort_key, reverse=self.treeview_sort_reverse)
        elif col_type == "boolean":
             # Sort boolean (Yes/No/Empty) - e.g., Yes=1, No=0, Empty=-1
             def bool_sort_key(item):
                 value = item[0]
                 if value == "Yes": return 1
                 elif value == "No": return 0
                 else: return -1 # Treat empty as lowest
             data.sort(key=bool_sort_key, reverse=self.treeview_sort_reverse)
        else: # Default to text sorting
            data.sort(key=lambda x: str(x[0]).lower(), reverse=self.treeview_sort_reverse) # Ensure comparison is lower case string


        # Update the treeview with the sorted data
        for index, (val, child) in enumerate(data):
            self.tree.move(child, '', index) # Move item to its new position


        # Update the column headings to show the sort indicator (arrow)
        for c_key, config in self.tree_columns_config.items():
            if config.get("visible", True):
                text = config["text"]
                if c_key == col:
                    # Add arrow indicator to the sorted column
                    arrow = ' ↑' if not self.treeview_sort_reverse else ' ↓'
                    self.tree.heading(c_key, text=f"{text}{arrow}")
                else:
                    # Remove arrow from other columns
                    self.tree.heading(c_key, text=text)


    # --- Mapping Functions (Threaded Geocoding) ---

    def geolocate_city_state(self, city, state):
        """Geolocates a city and state using Nominatim and returns latitude, longitude."""
        if not city or not state:
            return None # Cannot geolocate without city and state

        location_string = f"{city}, {state}, USA"
        try:
            # Use the thread's geolocator instance if available, otherwise main thread one
            # It's safer to create the geolocator instance *within* the thread function itself
            # So, this method won't use self._thread_geolocator, the thread function will
            # This method is effectively only called from the threaded function now.

            # Add a try/except around the geolocator.geocode call itself
            # Use the geolocator instance passed to or created in the thread function
            # Since this method is called from _geocode_locations_in_thread, it uses the geolocator from there.
            # The Nominatim instance is created in _geocode_locations_in_thread.

            location = Nominatim(user_agent=APP_NAME).geocode(location_string, timeout=10) # Create temporary instance or pass one


            if location:
                logging.debug(f"Geolocated '{location_string}': {location.latitude}, {location.longitude}")
                return location.latitude, location.longitude
            else:
                logging.warning(f"Could not geolocate: {location_string}")
                return None
        except GeocoderTimedOut:
             logging.warning(f"Geocoding timed out for: {location_string}")
             return None
        except GeocoderUnavailable:
             logging.warning(f"Geocoding service unavailable for: {location_string}")
             return None
        except Exception as e:
            logging.error(f"Error during geocoding call for '{location_string}': {e}")
            return None


    def load_map_markers(self):
        """Clears existing markers and starts the geocoding process for unique locations in a separate thread."""
        if not hasattr(self, 'map_widget') or not self.map_widget:
             logging.warning("Map widget not initialized when trying to load markers.")
             self.update_status("Map widget not available.")
             return

        # Check if the map widget still exists before interacting with it
        if not self.map_widget.winfo_exists():
             logging.warning("Map widget already destroyed when trying to load markers.")
             self.update_status("Map widget not available.")
             return


        # Check if a geocoding thread is already running
        if self.geocoding_thread and self.geocoding_thread.is_alive():
             logging.info("Geocoding thread is already running, skipping new load request.")
             self.update_status("Map loading already in progress.")
             return # Do not start a new thread if one is active

        self.update_status("Loading map markers (Geocoding in progress)...")
        self.map_widget.delete_all_marker() # Clear existing markers immediately
        self.map_markers = {} # Clear the dictionary for markers by location key
        self.geolocated_count = 0 # Reset counts
        self.skipped_count = 0
        self._grouped_cases_by_location = {} # Clear grouped data from previous load

        all_cases = get_all_cases_db() # Fetch all cases in the main thread initially

        if not all_cases:
             self.update_status("Map status: No cases to geocode.")
             logging.info("Map: No cases to geocode.")
             # Ensure map widget is not left blank
             self.map_widget.set_position(32.7, -89.5) # Center on MS
             self.map_widget.set_zoom(7)
             return

        # Group cases by city and state
        unique_locations = set()
        for case in all_cases:
             city = (case.get("city_of_offense") or "").strip()
             state = (case.get("state_of_offense") or "").strip()
             if city and state:
                  location_key = (city, state)
                  unique_locations.add(location_key)
                  # Group cases for later use in info bubble
                  if location_key not in self._grouped_cases_by_location:
                       self._grouped_cases_by_location[location_key] = []
                  self._grouped_cases_by_location[location_key].append(case)

        list_of_unique_locations = list(unique_locations)


        if not list_of_unique_locations:
             self.update_status("Map status: No geolocatable cases (missing city/state).")
             logging.info("Map: No geolocatable cases (missing city/state).")
             self.map_widget.set_position(32.7, -89.5) # Center on MS
             self.map_widget.set_zoom(7)
             return


        # Initialize the queue
        self.geocoding_queue = queue.Queue()
        self.processing_queue = True # Set flag to indicate we should process the queue


        # Create and start the geocoding thread
        # Pass the list of unique locations and the queue
        self.geocoding_thread = threading.Thread(target=self._geocode_locations_in_thread, args=(list_of_unique_locations, self.geocoding_queue))
        self.geocoding_thread.daemon = True # Allow the application to exit even if the thread is running
        self.geocoding_thread.start()

        # Start checking the queue for results periodically in the main thread
        self._process_geocoding_results() # Call the processing method


    # Inside CaseLogApp class
    def _geocode_locations_in_thread(self, locations, result_queue):
        """Performs geocoding for a list of unique locations and puts results in a queue."""
        logging.info(f"Geocoding thread started. Processing {len(locations)} unique locations.")
        thread_geolocator = Nominatim(user_agent=APP_NAME)

        for index, location_tuple in enumerate(locations): # locations is a list of (city, state) tuples
            city, state = location_tuple
            location_cache_key = f"{city}|{state}" # Create a consistent cache key

            # Check if the thread is asked to stop
            if not getattr(self.root, '_running', True):
                 logging.info("Geocoding thread received stop signal.")
                 break

            # 1. Check cache first
            cached_coords = get_cached_location_db(location_cache_key)
            if cached_coords:
                latitude, longitude = cached_coords
                result_queue.put(('success_cached', city, state, latitude, longitude))
                # logging.debug(f"Thread: Found cached location '{location_cache_key}'.")
                # time.sleep(0.01) # Optional small delay if processing many cache hits
                continue # Move to the next location

            # 2. If not in cache, geocode using Nominatim
            # logging.debug(f"Thread: Geocoding '{city}, {state}' via Nominatim.")
            location_string = f"{city}, {state}, USA"
            try:
                location = thread_geolocator.geocode(location_string, timeout=10)

                if location:
                    # Add to cache
                    add_cached_location_db(location_cache_key, location.latitude, location.longitude)
                    result_queue.put(('success_geocoded', city, state, location.latitude, location.longitude))
                    # logging.debug(f"Thread: Geolocated and cached '{location_cache_key}'.")
                else:
                    result_queue.put(('skipped', city, state, "Could not geolocate via Nominatim"))
                    # logging.debug(f"Thread: Could not geolocate '{location_string}'.")
            except GeocoderTimedOut:
                result_queue.put(('skipped', city, state, "Geocoding timed out"))
                logging.warning(f"Thread: Geocoding timed out for '{location_string}'.")
            except GeocoderUnavailable:
                result_queue.put(('skipped', city, state, "Geocoding service unavailable"))
                logging.warning(f"Thread: Geocoding service unavailable for '{location_string}'.")
            except Exception as e:
                result_queue.put(('skipped', city, state, f"Nominatim Error: {e}"))
                logging.error(f"Thread: Error during Nominatim geocoding for '{location_string}': {e}")

            # Nominatim usage policy: max 1 request per second.
            time.sleep(1.1) # Adhere to Nominatim's usage policy (1 req/sec)

        result_queue.put(('finished',))
        logging.info("Geocoding thread finished.")


    # Inside CaseLogApp class
    def _process_geocoding_results(self):
        """Checks the geocoding result queue and updates the map in the main thread."""
        # ... (cancel previous after call, and safety checks for root/map_widget) ...

        try:
            while True:
                try:
                    item = self.geocoding_queue.get_nowait()
                except queue.Empty:
                    break

                if item[0] == 'finished':
                    logging.info("Received 'finished' signal from geocoding thread.")
                    self.processing_queue = False
                    self.geocoding_thread = None
                    self._finalize_map_loading()
                    # The loop will now exit due to processing_queue = False or break below

                # Check if map widget still exists before processing success/skipped items
                if not hasattr(self, 'map_widget') or not self.map_widget or not self.map_widget.winfo_exists():
                    logging.warning(f"Map widget destroyed while processing queue item: {item[0]}. Skipping further processing in this cycle.")
                    self.processing_queue = False # Stop further processing if map is gone
                    break # Exit while loop

                # Handle new success types for cached and newly geocoded, plus original 'success' for compatibility
                elif item[0] in ('success_cached', 'success_geocoded', 'success'): # 'success' for backward compatibility
                    status_type, city, state, latitude, longitude = item
                    location_key_tuple = (city, state) # This is the tuple (city,state) used for _grouped_cases_by_location

                    # --- This is the info_text logic from the PREVIOUS response ---
                    # --- Ensure this part is correctly updated as per the previous response ---
                    city_of_offense = city # city comes from the queue item
                    info_text = f"City of Offense: {city_of_offense}\n"
                    cases_at_location = self._grouped_cases_by_location.get(location_key_tuple, []) # location_key_tuple is (city,state)
                    if cases_at_location:
                        unique_offense_types = set()
                        for case in cases_at_location:
                            offense_type = (case.get('offense_type') or '').strip()
                            if offense_type:
                                unique_offense_types.add(offense_type)
                        if unique_offense_types:
                            info_text += "\nTypes of Offense:\n"
                            for offense in sorted(list(o_type for o_type in unique_offense_types if o_type)): # Filter out empty strings if any
                                info_text += f"- {offense}\n"
                        else:
                            info_text += "\nNo specific offense types listed for this city."
                    else:
                        info_text += "\nNo case data found for this location."
                    
                    info_text_for_popup = info_text.strip() # Final string to be shown in popup
                    # --- End of info_text logic ---

                    marker_icon_to_use = DEFAULT_MARKER_ICON
                    try:
                        # MODIFIED MARKER CREATION:
                        marker = self.map_widget.set_marker(
                            latitude,
                            longitude,
                            text="",  # Explicitly set to empty string to avoid persistent labels
                            icon=marker_icon_to_use,
                            command=self.on_marker_click,  # Assign our new click handler
                            data=info_text_for_popup      # Store the info string in marker.data
                        )

                        self.map_markers[location_key_tuple] = marker # location_key_tuple is (city, state)
                        self.geolocated_count += 1
                        
                        log_message_suffix = "from cache" if status_type == 'success_cached' else "after geocoding"
                        logging.debug(f"Main thread: Set marker for '{city}, {state}' {log_message_suffix} with click command.")
                        
                        self.update_status(f"Loading map markers... ({self.geolocated_count} locations processed, {self.skipped_count} skipped)")
                        if getattr(self.root, '_running', True):
                             self.root.update_idletasks()
                    except Exception as e:
                        logging.error(f"Main thread: Error setting map marker for '{city}, {state}': {e}")
                        self.skipped_count += 1
                
                elif item[0] == 'skipped':
                    status, city, state, reason = item
                    logging.debug(f"Main thread: Location '{city}, {state}' skipped ({reason}).")
                    self.skipped_count += 1
                    self.update_status(f"Loading map markers... ({self.geolocated_count} locations processed, {self.skipped_count} skipped)")
                    if getattr(self.root, '_running', True):
                         self.root.update_idletasks()

        except Exception as e:
             logging.error(f"Unexpected error in _process_geocoding_results: {e}")
             self.processing_queue = False
             if getattr(self.root, '_running', True):
                  self.update_status("Error processing map data.")
        
        if self.processing_queue and getattr(self.root, '_running', True):
             if hasattr(self, 'map_widget') and self.map_widget and self.map_widget.winfo_exists():
                 self._geocoding_after_id = self.root.after(50, self._process_geocoding_results)
             else:
                 logging.warning("Map widget destroyed, not rescheduling _process_geocoding_results.")
                 self.processing_queue = False


    # New method to finalize map loading after threading
    def _finalize_map_loading(self):
        """Performs final map updates (fitting markers, setting final status) in the main thread."""
        logging.info("Finalizing map loading.")
        # Check if the map widget still exists before trying to interact with it
        if hasattr(self, 'map_widget') and self.map_widget and self.map_widget.winfo_exists():
             if self.geolocated_count > 0:
                 try:
                     # Get coordinates of all placed markers to fit the map
                     marker_coords = [(m.position[0], m.position[1]) for m in self.map_markers.values()]
                     if marker_coords: # Only fit if there are markers
                         self.map_widget.fit_markers()
                         final_map_status = f"Map status: Displaying {self.geolocated_count} locations with markers."
                         logging.info(f"Map: Displaying {self.geolocated_count} locations with markers.")
                     else:
                         final_map_status = "Map status: No geolocated markers to display."
                         logging.info("Map: No geolocated markers to display.")
                         # Reset map view
                         self.map_widget.set_position(32.7, -89.5) # Center on MS
                         self.map_widget.set_zoom(7)


                 except Exception as e:
                      logging.warning(f"Map: Could not fit markers: {e}")
                      final_map_status = f"Map status: Displaying {self.geolocated_count} locations. Could not fit view."
             else:
                 final_map_status = "Map status: No geolocatable cases."
                 logging.info("Map: No geolocatable cases.")
                 # Set a default view if no markers
                 try:
                      self.map_widget.set_position(32.7, -89.5) # Center on MS if empty
                      self.map_widget.set_zoom(7) # Default zoom level
                 except Exception as e:
                      logging.warning(f"Could not set default map view: {e}")


             if self.skipped_count > 0:
                  logging.info(f"Map: Skipped {self.skipped_count} locations (missing location or geocoding failed).")
                  # Add skipped count to final status message if any were skipped
                  final_map_status += f" ({self.skipped_count} skipped/errored)"


             self.map_status_label.config(text=final_map_status)
             self.update_status("Map markers loaded.")
        else:
             logging.warning("Map widget not available or destroyed during finalization.")
             # Status bar update might also fail if root is destroyed, but try anyway.
             try:
                 self.update_status("Map finalization skipped (widget destroyed).")
             except Exception:
                  pass # Ignore errors if status bar is already gone


        # Ensure the status bar is reset after completion
        # Only set to Ready if the status is related to map loading or generic
        # Add a check if status_label still exists
        if hasattr(self, 'status_label') and self.status_label and self.status_label.winfo_exists():
            if self.status_text.startswith("Map markers loaded") or self.status_text.startswith("Loading map markers") or self.status_text == "Initializing application...":
                 self.update_status("Ready")
        else:
             logging.debug("Status label not available or destroyed during final status update.")


    # --- Graphing Functions ---

    def populate_graph_filters(self):
        """Populates the year filter combobox based on available data."""
        self.update_status("Updating graph filters...")
        self.root.update_idletasks()

        cases = get_all_cases_db()
        years = set()
        for case in cases:
            created_at_str = case.get("created_at")
            if created_at_str:
                try:
                    # Parse theYYYY-MM-DD HH:MM:SS timestamp
                    created_date = datetime.strptime(str(created_at_str), '%Y-%m-%d %H:%M:%S').date()
                    years.add(str(created_date.year))
                except (ValueError, TypeError):
                    logging.warning(f"Could not parse created_at date '{created_at_str}' for graphing filter.")
                    pass # Ignore unparseable dates

        # Sort years and add "All" option
        sorted_years = sorted(list(years))
        filter_values = ["All"] + sorted_years

        # Store current selected year if possible
        current_year_selection = self.graph_year_var.get()

        # Update the combobox values
        self.graph_year_combo['values'] = filter_values

        # Attempt to restore previous selection, otherwise set to "All"
        if current_year_selection in filter_values:
             self.graph_year_var.set(current_year_selection)
        else:
             self.graph_year_var.set("All") # Default to showing all years


        # Trigger a graph update after updating filters
        self.update_graph()
        self.update_status("Graph filters updated.")


    def update_graph(self):
        """Generates and displays the selected graph based on filters."""
        # Only update if self.ax and self.canvas_agg exist (i.e., create_graph_widgets has run)
        if not hasattr(self, 'ax') or not self.ax or not hasattr(self, 'canvas_agg') or not self.canvas_agg:
             logging.warning("Graph widgets not initialized when update_graph called.")
             return


        self.update_status("Generating graph...")
        self.root.update_idletasks()

        selected_type = self.graph_type_var.get()
        selected_year = self.graph_year_var.get()

        cases = get_all_cases_db()

        # Filter cases by year if a specific year is selected
        if selected_year != "All":
            try:
                filter_year = int(selected_year)
                filtered_cases = [
                    case for case in cases
                    if case.get("created_at")
                    and isinstance(case["created_at"], str) # Ensure it's a string before parsing
                    and datetime.strptime(case["created_at"], '%Y-%m-%d %H:%M:%S').date().year == filter_year
                ]
                cases_to_graph = filtered_cases
            except (ValueError, TypeError) as e:
                logging.error(f"Error filtering cases by year '{selected_year}': {e}. Showing all cases.")
                cases_to_graph = cases # Fallback to showing all if filter fails
        else:
            cases_to_graph = cases # Show all cases if "All" is selected


        # Prepare data based on selected graph type
        # Map the display text back to the database key
        graph_type_mapping = {
            "Offense Type": "offense_type",
            "Device Type": "device_type",
            "OS": "os",
            "Agency": "agency",
            "State of Offense": "state_of_offense"
        }
        db_key = graph_type_mapping.get(selected_type, "offense_type") # Default to offense_type if key not found


        data_to_count = [case.get(db_key) for case in cases_to_graph]
        # Replace None or empty strings with a category like "Unknown"
        data_to_count = [str(item).strip() if item is not None and str(item).strip() else "Unknown" for item in data_to_count]


        # Count occurrences of each category
        counts = pd.Series(data_to_count).value_counts()

        # Clear the previous plot
        self.ax.clear()

        if counts.empty:
            self.ax.text(0.5, 0.5, "No data available for this selection.", horizontalalignment='center', verticalalignment='center', transform=self.ax.transAxes)
            self.ax.set_title("Graph")
            # Ensure the axis ticks and labels are cleared as well if no data
            self.ax.set_xlabel("")
            self.ax.set_ylabel("")
            plt.xticks([])
            plt.yticks([])
            self.canvas_agg.draw()
            self.update_status("Graph updated: No data.")
            logging.info("Graph updated: No data available for selected filters.")
            return


        # Create the plot based on the counts
        counts.plot(kind='bar', ax=self.ax, color='skyblue')

        # Set title and labels
        self.ax.set_title(f"Case Count by {selected_type} ({selected_year if selected_year != 'All' else 'All Years'})")
        self.ax.set_xlabel(selected_type)
        self.ax.set_ylabel("Number of Cases")

        # Rotate x-axis labels for readability if many categories
        plt.xticks(rotation=45, ha='right')

        # Add value labels on top of bars
        # Ensure text placement is correct for potentially rotated labels
        for i, count in enumerate(counts):
            self.ax.text(i, count + (counts.max() * 0.01), str(count), ha='center', va='bottom') # Adjust text position based on max count


        # Adjust layout to prevent labels overlapping
        self.fig.tight_layout()

        # Draw the plot on the canvas
        self.canvas_agg.draw()

        self.update_status("Graph updated.")
        logging.info(f"Graph updated: {selected_type} for {selected_year}.")


    # --- Status Bar Functions ---
    def update_status(self, message):
        """Updates the text in the status bar."""
        self.status_text = message
        # Only update the label config if the status_label widget has been created and still exists
        if hasattr(self, 'status_label') and self.status_label and self.status_label.winfo_exists():
            self.status_label.config(text=message)
            # Cancel any ongoing animation if text changes
            if self.status_animation_id:
                self.root.after_cancel(self.status_animation_id)
                self.status_animation_id = None

    def start_status_animation(self):
        """Starts a simple animation in the status bar."""
        # Only start animation if status_label exists and is not destroyed
        if not hasattr(self, 'status_label') or not self.status_label or not self.status_label.winfo_exists():
             return

        if self.status_animation_id: # Avoid starting multiple animations
            return

        def animate(frame=0):
            # Check if root is still running and status_label exists before updating
            if not getattr(self.root, '_running', True) or not hasattr(self, 'status_label') or not self.status_label or not self.status_label.winfo_exists():
                 self.status_animation_id = None # Stop animation if app is closing or widget destroyed
                 return # Exit the animate function

            animation_frames = ["|", "/", "-", "\\"]
            current_text = self.status_label.cget("text")
            # Only animate if the status text hasn't changed manually and still starts with the base text
            if current_text.startswith(self.status_text):
                 animated_text = f"{self.status_text} {animation_frames[frame % len(animation_frames)]}"
                 if hasattr(self, 'status_label') and self.status_label and self.status_label.winfo_exists(): # Final check before updating widget
                     self.status_label.config(text=animated_text)
                     self.status_animation_id = self.root.after(100, animate, frame + 1) # Schedule next frame
            else:
                 self.status_animation_id = None # Stop animation if text changed

        self.status_animation_id = self.root.after(0, animate, 0) # Start the animation immediately

    def stop_status_animation(self):
        """Stops the status bar animation."""
        if self.status_animation_id:
            self.root.after_cancel(self.status_animation_id)
            self.status_animation_id = None
            # Only restore text if status_label exists, is not destroyed, and text is currently animated
            if hasattr(self, 'status_label') and self.status_label and self.status_label.winfo_exists() and self.status_label.cget("text").startswith(self.status_text):
                self.status_label.config(text=self.status_text) # Restore original text


    def on_closing(self):
        """Handles cleanup when the application window is closed."""
        logging.info("Application closing initiated.")
        # Set the running flag to False to signal threads to stop
        self.root._running = False

        # Cancel the periodic check for geocoding results if it's scheduled
        if self._geocoding_after_id:
            logging.info(f"Cancelling geocoding after ID: {self._geocoding_after_id}")
            try:
                self.root.after_cancel(self._geocoding_after_id)
            except tk.TclError as e:
                 # Catch TclError if the ID is already invalid (e.g., widget destroyed)
                 logging.debug(f"TclError cancelling geocoding after ID {self._geocoding_after_id}: {e}")
            except Exception as e:
                 logging.error(f"Unexpected error cancelling geocoding after ID {self._geocoding_after_id}: {e}")
            self._geocoding_after_id = None # Clear the stored ID

        # Explicitly destroy the map widget to try and stop its internal processes
        if hasattr(self, 'map_widget') and self.map_widget and self.map_widget.winfo_exists():
            logging.info("Destroying map widget...")
            try:
                self.map_widget.destroy()
                logging.info("Map widget destroyed.")
            except Exception as e:
                logging.error(f"Error destroying map widget: {e}")

        # Add a small sleep to potentially allow pending events to clear
        logging.debug("Adding small sleep before destroying root...")
        time.sleep(0.1) # Sleep for 100 milliseconds

        # Optional: Add logic here to wait briefly for the geocoding thread to finish
        # if self.geocoding_thread and self.geocoding_thread.is_alive():
        #     logging.info("Waiting for geocoding thread to join...")
        #     # Use a short timeout to avoid blocking shutdown indefinitely
        #     self.geocoding_thread.join(timeout=1.0)
        #     if self.geocoding_thread.is_alive():
        #         logging.warning("Geocoding thread did not finish within timeout.")
        #     else:
        #         logging.info("Geocoding thread joined successfully.")


        # Destroy the main window
        logging.info("Destroying main window...")
        self.root.destroy()
        logging.info("Main window destroyed.")


# --- Main Execution ---
if __name__ == "__main__":
    # Wrap the application startup in a try...except block to catch early errors
    try:
        init_db() # Initialize DB and logging before starting UI

        root = tk.Tk()
        # Set a flag on root to signal threads to stop if the app is closing
        root._running = True
        # Set the window closing protocol later in __init__ now that the app instance exists


        app = CaseLogApp(root) # Create the application instance

        root.mainloop() # Start the Tkinter event loop

    except Exception as e:
        # Catch any exception that occurs during initialization or before mainloop is stable
        logging.exception("An unhandled exception occurred during application startup:") # Log the exception with traceback
        print("\nAn unhandled exception occurred during application startup:")
        print(e)
        import traceback
        traceback.print_exc() # Print the traceback to the console

        # Optionally, show a simple error message box if Tkinter root was created
        # Add a check if root still exists before trying to show a messagebox
        if 'root' in locals() and isinstance(root, tk.Tk) and root.winfo_exists():
             try:
                  messagebox.showerror("Application Error", f"An unexpected error occurred during startup:\n{e}\nCheck console and app.log for details.")
             except Exception: # Handle potential errors showing the messagebox itself
                  pass # Fail silently if messagebox cannot be shown

    finally:
        # Any cleanup code can go here if needed
        logging.info("Application shutting down.")
        # Ensure log handlers are flushed/closed on exit
        # This might be handled by logging.shutdown() which is automatically called on exit
        # Forcing it here might be redundant or cause issues depending on environment
        # logging.shutdown()
        pass # No specific cleanup needed here for this app structure