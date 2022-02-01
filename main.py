# A python program to download historic of Derivatives from NSE.
# The downloaded data can be sorted by 'INSTRUMENTS' and 'SYMBOL'
# If the required data is missing, the program will download it.
# Already downloaded data can also be used to sort/filter.
# The filtered csv file will be in the same location as downloaded data.

# Required imports
import requests
import zipfile
import calendar
import csv
import os

instrument = input("Enter the instrument: ").upper()
symbol = input("Enter the Symbol: ").upper()

startMonth = int(input("Enter the number of start month (1-Jan, 2-Feb...): "))
endMonth = int(input("Enter the number of end month (1-Jan, 2-Feb...): "))
year = int(input("Enter the year: "))

dataDir = input("Enter the name of download directory: ")

# If the folder for where the data has to be downloaded does not exist, the program creates the folder.
if not os.path.exists(dataDir):
    os.mkdir(dataDir)

# Base URL for the data.
base_url = "https://www1.nseindia.com/content/historical/DERIVATIVES/" + str(year) + "/"

# Loop for the month variable.
for i in range(startMonth, endMonth+1):

    # This is required to find the number of days in a particular month.
    x, days = calendar.monthrange(year, i)

    # Loop for days variable.
    for d in range(1, days + 1):

        # To skip weekends as the file does not exist.
        if calendar.weekday(year, i, d) in [0, 1, 2, 3, 4]:

            # For the file name, we require names of months in their abbreviation.
            monthAbbr = calendar.month_abbr[i].upper()
            # variable to store the file name.
            filename = dataDir + "/fo" + str(d).zfill(2) + monthAbbr + str(year) + "bhav.csv.zip"
            print(filename)

            # if the file is not downloaded, then download it.
            if not os.path.exists(filename.replace(".zip", "")):

                # URL for the file in NSE server.
                new_url = base_url + monthAbbr + "/fo" + str(d).zfill(2) + monthAbbr + str(year) + "bhav.csv.zip"
                print(d, new_url)
                # Send the request.
                r = requests.get(new_url, stream=True)
                # Write the received data to .zip file.
                with open(filename, 'wb') as code:
                    code.write(r.content)
                # Next, try to unzip the file and then delete the .zip file after extraction.
                try:
                    with zipfile.ZipFile(filename, 'r') as zip_ref:
                        zip_ref.extract(filename.replace(".zip", "").replace(dataDir+"/", ""), path="data")
                    if os.path.exists(filename.replace(".zip", "")):
                        os.remove(filename)

                # There won't be a proper .zip file in case of festivals. So to catch the BadZipFile error.
                # Then delete the bad ip file.
                except zipfile.BadZipfile:
                    if os.path.exists(filename):
                        os.remove(filename)
                    print("not zip")

            # Check for file and then proceed to filter.
            if os.path.exists(filename.replace(".zip", "")):
                # Open 2 files: downloaded file with read permissions and another file with append permissions.
                # The data for different months will be appended to the same file with name INSTRUMENT_SYMBOL_bhavcopy.csv
                with open(filename.replace(".zip", ""), 'r') as fin, open(dataDir + '/' + instrument + '_' + symbol + '_' + 'bhavcopy.csv', 'a') as fout:
                    writer = csv.writer(fout, delimiter=",")
                    # We add the date, month, year after each file filtering.
                    writer.writerow([d, monthAbbr, year])
                    for row in csv.reader(fin, delimiter=","):
                        if row[0] == instrument and row[1] == symbol:
                            writer.writerow(row)
