# Vehicle Recall Scraper
vehicle recall scraper which automatically collects information about vehicle
## Installation

Install Python: If Python is not already installed on your system, download and install the latest version of Python from the official Python website (https://www.python.org/downloads/).

 Install Required Packages: install them using the pip package manager [pip](https://pip.pypa.io/en/stable/) to install the requirements.
```bash
pip install -r /path/to/requirements.txt
```

## Usage

There are two folders which contains Ford and Chvrolet scraping code and files
1. Ford:    

      **ford_vehicle_recall_data_scraper.py** is a python file that will scrape all the required information including the Customer Satisfaction Programs data 
      
      **scrape_recall_status.py** scrapes only the Recall Status it is built using selenium (requires chrome driver to be installed and in the path) to scrape the details 

      **vehicle_record_data_ford.csv** is the final csv with all the details

      **vin_nos_ford** is the input file that contains vin numbers for the Ford scraper

2. Chevrolet: 

      **chevrolet_vehicle_recall_data_scraper.py** is a python that file will scrape all the required  data 
      
      **vehicle_record_data_chevrolet.csv** is the final csv with all the details
      
      **vin_nos_chevrolet** is the input file that contains vin numbers for the Chevrolet scraper
      
## Run the Web Scraper
Run the Python web scraper by entering the command python 
``` <filename.py> ``` in the terminal/command prompt window. Replace <filename.py> with the name of the Python file containing the web scraper code.

