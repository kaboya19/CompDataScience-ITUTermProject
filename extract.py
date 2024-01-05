import csv

def extract_data(ind_file, hld_file):
    list_data_ind = []
    list_data_hld = []

    try:
        # Extract Individual Data
        with open(ind_file, 'r', encoding='utf-8') as ind_csv_file:
            ind_csv_reader = csv.DictReader(ind_csv_file)
            for row in ind_csv_reader:
                list_data_ind.append(row)

        # Extract Household Data
        with open(hld_file, 'r', encoding='utf-8') as hld_csv_file:
            hld_csv_reader = csv.DictReader(hld_csv_file)
            for row in hld_csv_reader:
                list_data_hld.append(row)

    except FileNotFoundError as e:
        print(f"File not found: {e.filename}")
    except Exception as e:
        print(f"An error occurred: {e}")

    return list_data_ind, list_data_hld

# Example usage:
ind_file = 'WLD_2023_SYNTH-SVY-IND-EN_v01_M.csv'
hld_file = 'WLD_2023_SYNTH-SVY-HLD-EN_v01_M.csv'

ind_data, hld_data = extract_data(ind_file, hld_file)
print("Individual Data:")
print(ind_data[:5])

print("\nHousehold Data:")
print(hld_data[:5])

