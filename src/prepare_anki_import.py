import pandas as pd
import os
from sys import argv
from bs4 import BeautifulSoup
from datetime import datetime
from pathlib import Path
from logging import basicConfig, debug, DEBUG
# basicConfig(level=DEBUG)

lookup_path = Path("C:/Users/marks/coding/anki_csv_maker/tables/search_index.csv.gz")
dict_path = Path("C:/Users/marks/coding/anki_csv_maker/tables/anki_entries.csv.gz")
default_save_absolute = Path("G:/My Drive/learning/Languages/日本語/anki import")
default_save_relative = (Path.cwd() / ".." / "anki import").resolve()

def main():
    word_list_path, output_path = get_parameters()
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
        debug("exit search loop")
        entries.extend(new_entries)
                
    debug(entries)
    anki_table = anki_entries.loc[entries]
    full_output(anki_table)
    try:
        size = int(input("Enter max words per input, 0 to save all together, or any non-number to cancel: "))
        save_tables(anki_table, output_path, size)
    except:
        print("Did not save tables.")

def get_parameters():
    if len(argv) == 2:
        print(f"Defaulting save path to {default_save_absolute}")
        output_path = default_save_absolute
        word_list_path = Path(argv[1])
    elif len(argv) == 3:
        word_list_path, output_path = map(Path, argv[1:3])
    else:
        readable_files = [filename for filename in os.listdir(Path.cwd()) if filename[-4:] in [".txt", "html"]]
        if not readable_files:
            print("No readable files in current directory.")
            exit()
        chosen_index = None
        while chosen_index is None:
            for i, filename in enumerate(readable_files):
                print(i+1, filename)
            try:
                choice = int(input("Choose file, or 0 to exit: ").strip())
            except ValueError:
                print("Input not a valid number")
                continue
            if choice > 0 and choice <= len(readable_files):
                chosen_index = choice - 1
            elif choice == 0:
                exit()
            else:
                print("Input out of range")
                continue
        word_list_path = Path.cwd() / readable_files[chosen_index]
        output_path = None
        while output_path is None:
            output_choice_str = input(f"Enter save path, or blank to use {default_save_relative}\n").strip()
            if output_choice_str:
                try:
                    output_choice = Path(output_choice_str)
                except ValueError:
                    print("Could not interpret as path")
                    continue
                if not output_choice.is_dir():
                    print("Save path must be a directory")
                    continue
                if not output_choice.exists():
                    print("Save path must already exist")
                    continue
                output_path = output_choice
            else:
                output_path = default_save_absolute
    return word_list_path, output_path


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


def read_word_list(path: Path):
    _, filetype = os.path.splitext(path)
    if filetype == '.txt':
        with open(path, encoding='utf8') as f:
            words = [l.strip() for l in f]
    elif filetype == '.html':
        with open(path, encoding='utf8') as f:
            words = [
                entry.text.split('\n')[0] for entry in
                BeautifulSoup(f, features='html.parser').find_all("div", {'class':'noteText'})
            ]
    else:
        raise NotImplementedError(f'Cannot handle {filetype} files.')
    return words


def get_matches(search_index:pd.DataFrame, word:str) -> list[int]:
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
