from rest_api_pipeline import load_tfl_data
from filesystem_pipeline import main_load

if __name__ == "__main__":
    tables = load_tfl_data()
    main_load(tables)
