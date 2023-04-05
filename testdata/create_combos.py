# open csv
import csv
from itertools import chain, combinations
import json

MIN_KEYS = 2
# Minimum number of keys to combine

def all_subsets(ss):
    return chain(*map(lambda x: combinations(ss, x), range(MIN_KEYS, len(ss)+1)))

def fieldset_to_str(field_set):
    field_str = ""
    for fi in field_set:
        field_str +=f"{fi} ,"
    return field_str

def remove_null_columns(records,og_cols):
    nullable_cols =[]
    for rec in records:
        for key,val in rec.items():
            if val in (None, "", " "):
                nullable_cols.append(key)
    new_cols = set(og_cols) - set(nullable_cols)
    print(f"Using Column {len(new_cols)} / {len(og_cols)}")
    return new_cols

def analyse_file(filename):
    with open(filename,"r") as datafile:
        data  = csv.DictReader(datafile) 
        og_recs = list(data)
    sets = list(all_subsets(remove_null_columns(og_recs, data.fieldnames)))
    subset_len,dataset = len(sets), {}
    for indx, combo in enumerate(sets,1):
        print(f"Iterating set {indx} of {subset_len}",end='\r')
        field_str, records = fieldset_to_str(combo), []
        for row in og_recs:
            records.append(tuple(row[x] for x in combo))
        uniq_recs = set(records)
        dataset[field_str]={"total_rec":len(records),"red_rec":len(uniq_recs),"dups":len(records)-len(uniq_recs)}
    print("\n")
    dataset_sorted = dict(sorted(dataset.items(),key=lambda item: item[1]["dups"]))

    with open(f"{filename}.json","+w") as dwf:
        json.dump(dataset_sorted,dwf)

f = [
    "feature_events-20230405T121027.csv",
    "page_events-20230405T121027.csv",
    "track_events-20230405T121027.csv",
    "events-20230405T121027.csv",
    "guide_events-20230405T121027.csv",
    "poll_events-20230405T121027.csv"]

for fi in f:
    print("Starting File",fi)
    analyse_file(fi)