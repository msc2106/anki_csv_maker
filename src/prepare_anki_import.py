import pandas as pd
from sys import argv
from typing import List
from bs4 import BeautifulSoup
from datetime import datetime
from logging import basicConfig, debug, DEBUG
# basicConfig(level=DEBUG)

lookup_path = "C:/Users/marks/coding/anki_csv_maker/tables/search_index.csv.gz"
dict_path = "C:/Users/marks/coding/anki_csv_maker/tables/anki_entries.csv.gz"

def main():
    # load dictionaries (hardcoded)
    # load vocab list (1st argument)
    # for each item in list:
    ## if single match, save entry row to list
    ## if no match, prompt to adjust or skip
    ## if multiple match, prompt for which to add (one, a subset, all, or none) or to reenter
    # display full list, give option to adjust any (delete, re-search) or save
    # save to given path (2nd argument)
    
    if len(argv) != 3:
        print("Usage: prepare_anki_import.py [word list] [dest path]")
    word_list_path, output_path = argv[1:]
    search_index = pd.read_csv(lookup_path, index_col=0)
    anki_entries = pd.read_csv(dict_path, index_col=0)
    word_list = read_word_list(word_list_path)
    entries = []
    for word in word_list:
        debug(word)
        new_entries = None
        while new_entries is None:
            matches = get_matches(search_index, word)
            if len(matches) == 1:
                new_entries = matches
            elif len(matches) == 0:
                print(f'No match for {word}. Enter new search term or blank for to skip.')
                response = input().strip()
                if response:
                    word = response
                    continue
                else:
                    new_entries = []
            else:
                debug(matches)
                for i, match_id in enumerate(matches):
                    print(f'{i+1}: {anki_entries.loc[match_id, "expression"]} {anki_entries.loc[match_id, "meaning"]}')
                while True:
                    print("Multiple hits. Enter entries to use (separating multiple with spaces), or a for all, x for none, and r to refine the search term.")
                    response = input().strip()
                    if response == 'a':
                        new_entries = matches
                    elif response == 'x':
                        new_entries = []
                    elif response == 'r':
                        word = input("Enter new search term: ").strip()
                        continue
                    else:
                        try:
                            subset = [matches[idx-1] for idx in map(int, response.split())]
                        except:
                            continue
                        new_entries = subset
                    break # loop terminates unless explicitly continued
            debug("end search loop")
        debug('exit search loop')
        entries.extend(new_entries)
                
    debug(entries)
    anki_table = anki_entries.loc[entries]
    full_output(anki_table)
    try:
        size = int(input("Enter max words per input, 0 to save all together, or any non-number to cancel: "))
        save_tables(anki_table, output_path, size)
    except:
        print("Did not save tables.")


def save_tables(anki_table, output_path, size):
    entry_count = anki_table.shape[0]
    file_count = 1 if not size else entry_count // size + 1 
    if file_count == 1:
        output_file = f'{output_path}/{datetime.today().strftime("%Y.%m.%d")}.csv'
        anki_table.to_csv(output_file, index=False, header=False)
        print(f"Saved {output_file}")
    else:
        for i in range(file_count):
            start = i * size
            end = start + size
            output_file = f'{output_path}/{datetime.today().strftime("%Y.%m.%d")}.{i+1:02}.csv'
            anki_table.iloc[start:end, :].to_csv(output_file, index=False, header=False)
            print(f"Saved {output_file}")


def read_word_list(path):
    filetype = path.split('.')[-1]
    if filetype == 'txt':
        with open(path, encoding='utf8') as f:
            words = [l.strip() for l in f]
    elif filetype == 'html':
        with open(path, encoding='utf8') as f:
            words = [
                entry.text.split('\n')[0] for entry in
                BeautifulSoup(f, features='html.parser').find_all("div", {'class':'noteText'})
            ]
    else:
        raise NotImplementedError(f'Cannot handle {filetype} files.')
    return words


def get_matches(search_index:pd.DataFrame, word:str) -> List[int]:
    return list(search_index.id[search_index.entry == word])


def get_option(prompt, valid_responses):
    while True:
        print(prompt)
        response = input().strip()
        if response in valid_responses:
            break
    return response


def full_output(df: pd.DataFrame):
    cols = df.columns
    for i, row in df.iterrows():
        print(i, *(row[col] for col in cols))
    print(df.shape[0], "entries in total.")


if __name__ == '__main__':
    main()
