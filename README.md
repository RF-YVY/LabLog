# Cyber Lab Case Tracker

I built this as a personal want.  I had been keeping a log of activity of my lab on a spreadsheet, which worked great, and I had pretty graphs with it, but it had gotten to the point I was having to do more
scrolling to get to the next new entry slot than actually typing in my tracked data.  So I came up with this application that has a new entry screen right up front and still able to see all the data, generate graphs,
and generate a summary report.
![image](https://github.com/user-attachments/assets/eccad25e-2e75-4668-98cf-5f2a9b23c38f)
![image](https://github.com/user-attachments/assets/d6b7971d-3276-4e0c-af03-a803ef0c8137)
![image](https://github.com/user-attachments/assets/213bde6f-a66c-4032-94bd-cf1dafe1e437)
![image](https://github.com/user-attachments/assets/e0753854-96cb-4ac4-a0cc-7f0f18c3ea0b)
![image](https://github.com/user-attachments/assets/3f32f66b-b7a2-4f02-9fe0-2327da091f09)


## Purpose

The Case Log Tool is a desktop application designed to help users log and manage forensic case data. It provides features for:

* **New Case Entry:** Easily input details for new forensic cases.
* **View Data:** Browse, search (implicitly through data shown after filters/graphs), and sort existing case entries.
* **Map View:** Visualize case locations on a map based on city and state information.
* **Graph Analysis:** Generate dynamic graphs to analyze case data based on various criteria like examiner, agency, offense type, volume, and time.
* **Settings:** Manage application settings, including setting a header logo for reports and performing data management tasks like importing/exporting and clearing data.

The application stores data locally in a secure SQLite database.

## How to Use

1.  **Launch the Application:** Run the executable or script for the Case Log Tool.
2.  **Navigation:** Use the tabs at the top to switch between different sections: "New Case Entry", "View Data", "Map View", "Graph Analysis", and "Settings".
3.  **New Case Entry:**
    * Navigate to the "New Case Entry" tab.
    * Fill in the relevant fields (Examiner, Case #, Dates, Volume, Offense Type, Location, Device Info, Notes, etc.). Examiner and Cyber Case # are required fields.
    * Click the "Submit Case" button to save the entry.
4.  **View Data:**
    * Navigate to the "View Data" tab.
    * All case entries will be displayed in the table.
    * Click on column headers to sort the data.
    * Use the scrollbars to view all columns and rows.
    * Click "Refresh Data" to load the latest entries.
    * Use "Export All as PDF" or "Export All as XLSX" to save a report of the data.
5.  **Map View:**
    * Navigate to the "Map View" tab.
    * Click "Load/Refresh Map Markers" to display markers for cases that have city and state information. The map will attempt to geocode locations and cache them for faster loading in the future.
    * Click on a marker to see basic case information (Agency, Case #, Offense).
6.  **Graph Analysis:**
    * Navigate to the "Graph Analysis" tab.
    * Use the filter dropdowns (Examiner, Investigator, Agency) and Date Entry fields (Graph Start/End Date) to filter the data included in the graphs. Select "ALL" to include all data for that category.
    * Select a "Graph Type" from the dropdown (e.g., Case Counts, Total Volume, Device Counts, Volume by Device Type, Cases Over Time).
    * Select a "Group By" category from the dropdown. This determines how the selected graph type's data will be aggregated (e.g., show Case Counts *by* Examiner). The "Cases Over Time" graph does not use the "Group By" option.
    * Click "Generate Graphs" to display the chosen graph based on your selections and filters.
7.  **Settings:**
    * Navigate to the "Settings" tab.
    * **Report Header Logo:** Use "Select Logo File..." to choose an image (PNG, JPG, JPEG, GIF) to be used as a header logo on PDF reports and displayed in the "New Case Entry" tab. The logo is saved as 'logo.png' in the 'app_data' folder.
    * **Import Cases from XLSX:** Import case data from a standard Excel file (.xlsx). Ensure the column headers in your file match the expected fields (examiner, investigator, agency, case_number, start_date, end_date, volume_size_gb, offense_type, city_of_offense, state_of_offense, device_type, model, os, data_recovered, fpr_complete, notes).
    * **View Application Log:** Open a window displaying the application's log file ('app.log') for troubleshooting.
    * **Change Password:** Change the application's password (used for sensitive operations like clearing data). The default password is "1234".
    * **Clear Application Data:** **USE WITH CAUTION!** This button will permanently delete ALL saved case data and the logo file after requiring password confirmation.
    * **I added the password function to prevent accidental or unauthorized deletion of all data!  CHANGE PASSWORD!! (password prompt will likely hide behind main application window)
## Data Storage

Application data (case entries, geocoded locations, settings including password hash/salt, and the logo image) is stored locally in the `app_data` folder in the same directory as the application script/executable.

* `caselog_gui_v6.db`: The SQLite database file.
* `logo.png`: The selected report header logo image.
* `app.log`: The application log file.

## Importing of XLSX spreadsheet
  * If importing a exsisting XLSK spreadsheet, it must contain the following headers typed exactly as shown.
examiner , investigator , agency , case_number , start_date , volume_size_gb , offense_type , end_date , device_type , model , os , data_recovered , fpr_complete , notes , city_of_offense , state_of_offense
  * The "_" is critical where listed, city and state is critical for generating map markers.
## Customization
  * Use the LabLog.py file to make changes in your editor, I used Visual Studio Code.  Change defualt map view to your area/state or completely re-write the application code to your needs.
  * Please attribute the orginal work - ( Brett Wicker - brettwicker73@gmail.com )
