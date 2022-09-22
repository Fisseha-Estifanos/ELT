"""
A default script for all default variables
"""

# base repository
repo = '../'

# the path to the data folder
data_path = 'data/'

# setting up dvc data and folder structure paths
# main data file name
data_file_name = '20181024_d1_0830_0900.csv'

# the local data path
full_data_file = repo + data_path + data_file_name

# the path to the data set
data_file = data_path + data_file_name

# the path to the plots folder
plot_path = 'plots/'

# the path to the log files
log_path = repo + 'logs/'


# airflow related paths
# the data to download path - all drone data
path_to_source = 'https://open-traffic.epfl.ch/wp-content/uploads/mydownloads.php'

# the path to store the data to
path_to_store_data = '~/Documents/10X/week_V/project-folder/ELT/data/fetched_data.csv'
